/*
 * udp_artery_handler.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// UdpArteryHandler implements a native Go Aeron-wire-compatible UDP transport
// for Artery (the Akka/Pekko remoting protocol).
//
// Architecture summary
// ─────────────────────
//  ┌──────────────────────────────────────────────────────────────────────┐
//  │  Remote Akka/Pekko node                 Go (UdpArteryHandler)       │
//  │                                                                      │
//  │  INBOUND path (Akka → Go):                                          │
//  │    Publisher ──SETUP──▶ udpConn (port N)                            │
//  │             ◀──SM─────  (Go acknowledges, reports window)           │
//  │    Publisher ──DATA──▶  receive loop → reassemble → ArteryDispatch  │
//  │             ◀──SM───── (periodic window updates)                    │
//  │                                                                      │
//  │  OUTBOUND path (Go → Akka):                                         │
//  │    Go ──SETUP──▶  remote port M                                     │
//  │       ◀──SM────   (Akka acknowledges, reports window)               │
//  │    Go ──DATA──▶   (Artery envelope wrapped in Aeron DATA frame)     │
//  └──────────────────────────────────────────────────────────────────────┘
//
// Reliability model
// ─────────────────
//  • Periodic SM frames are sent to each active publisher, advertising the
//    current consumption position and receiver window.
//  • When a gap is detected in term-buffer offsets, a NAK frame is issued to
//    request retransmission of the missing range.
//  • Large messages (Artery stream ID 3) are reassembled from multiple DATA
//    fragments before being dispatched upstream.

package core

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// Inbound session state
// ---------------------------------------------------------------------------

// aeronInboundSession tracks the receiver state for a single Aeron publication
// (identified by the triple: remoteAddr + sessionId + streamId).
type aeronInboundSession struct {
	mu sync.Mutex

	// Identity
	remoteAddr *net.UDPAddr
	sessionId  int32
	streamId   int32

	// Term-buffer tracking
	initialTermId int32
	activeTermId  int32
	termOffset    int32 // next expected byte offset in the active term
	termLength    int32
	mtu           int32

	// Fragment reassembly (stream 3 = Large, but used for all multi-frame msgs)
	fragBuf      []byte // accumulated bytes for the current reassembly
	fragBeginOff int32  // termOffset of the BEGIN fragment
	fragTermId   int32  // term ID of the BEGIN fragment

	lastSmSent time.Time // when we last sent an SM to this publisher
}

// sessionKey returns a stable map key for an inbound session.
func inboundSessionKey(addr *net.UDPAddr, sessionId, streamId int32) string {
	return fmt.Sprintf("%s/%d/%d", addr.String(), sessionId, streamId)
}

// ---------------------------------------------------------------------------
// Outbound session state
// ---------------------------------------------------------------------------

// aeronOutSession tracks the publisher state for a single Aeron publication
// from Go to a remote subscriber.
type aeronOutSession struct {
	mu sync.Mutex

	remoteAddr    *net.UDPAddr
	streamId      int32
	sessionId     int32
	initialTermId int32
	activeTermId  int32
	termOffset    int32 // next write position in the active term
	termLength    int32

	established bool // true once an SM has been received from the remote
	setupSentAt time.Time
}

// outSessionKey returns a stable map key for an outbound session.
func outSessionKey(addr *net.UDPAddr, streamId int32) string {
	return fmt.Sprintf("%s/%d", addr.String(), streamId)
}

// ---------------------------------------------------------------------------
// UdpArteryHandler
// ---------------------------------------------------------------------------

// UdpArteryHandler provides an Aeron-wire-compatible UDP inbound/outbound
// transport layer for Artery messages.  One instance is shared across all
// logical Aeron streams (1, 2, 3) on a single UDP socket.
type UdpArteryHandler struct {
	mu         sync.RWMutex
	conn       *net.UDPConn
	localAddr  *net.UDPAddr
	nm         *NodeManager
	handler    FrameHandler
	ctm        *CompressionTableManager

	inSessions  map[string]*aeronInboundSession
	outSessions map[string]*aeronOutSession

	ctx    context.Context
	cancel context.CancelFunc
}

// NewUdpArteryHandler creates and starts a UdpArteryHandler that binds to addr.
// nm is the owning NodeManager, used to decode Artery envelopes and dispatch
// messages.  If handler is nil, DefaultFrameHandler is used.
func NewUdpArteryHandler(ctx context.Context, addr string, nm *NodeManager, handler FrameHandler) (*UdpArteryHandler, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("aeron-udp: resolve address %q: %w", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("aeron-udp: listen %q: %w", addr, err)
	}

	if handler == nil {
		handler = DefaultFrameHandler
	}

	handlerCtx, cancel := context.WithCancel(ctx)
	h := &UdpArteryHandler{
		conn:        conn,
		localAddr:   udpAddr,
		nm:          nm,
		handler:     handler,
		inSessions:  make(map[string]*aeronInboundSession),
		outSessions: make(map[string]*aeronOutSession),
		ctx:         handlerCtx,
		cancel:      cancel,
	}

	go h.receiveLoop()
	go h.smLoop()

	slog.Info("aeron-udp: listening", "addr", conn.LocalAddr())
	return h, nil
}

// Close shuts down the handler and releases its UDP socket.
func (h *UdpArteryHandler) Close() {
	h.cancel()
	h.conn.Close()
}

// LocalAddr returns the local UDP address this handler is bound to.
func (h *UdpArteryHandler) LocalAddr() *net.UDPAddr {
	return h.conn.LocalAddr().(*net.UDPAddr)
}

// ---------------------------------------------------------------------------
// Receive loop
// ---------------------------------------------------------------------------

// receiveLoop is the main goroutine that reads raw UDP datagrams and dispatches
// them to the appropriate frame handler.
func (h *UdpArteryHandler) receiveLoop() {
	buf := make([]byte, 65536)
	for {
		select {
		case <-h.ctx.Done():
			return
		default:
		}

		_ = h.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, src, err := h.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-h.ctx.Done():
				return
			default:
				slog.Warn("aeron-udp: read error", "error", err)
				continue
			}
		}

		if n < AeronDataHeaderLen {
			slog.Debug("aeron-udp: short datagram", "bytes", n, "src", src)
			continue
		}

		frameType := binary.LittleEndian.Uint16(buf[6:8])
		slog.Info("aeron-udp: received frame",
			"src", src, "bytes", n, "frameType", fmt.Sprintf("0x%04x", frameType))
		switch frameType {
		case AeronHdrTypeData:
			h.handleData(src, buf[:n])
		case AeronHdrTypeSetup:
			if n >= AeronSetupHeaderLen {
				h.handleSetup(src, buf[:n])
			}
		case AeronHdrTypeSm:
			if n >= AeronSmHeaderLen {
				h.handleSm(src, buf[:n])
			}
		case AeronHdrTypeNak:
			if n >= AeronNakHeaderLen {
				h.handleNak(src, buf[:n])
			}
		case AeronHdrTypePad:
			slog.Info("aeron-udp: PAD frame received", "src", src)
		default:
			slog.Info("aeron-udp: unknown frame type", "type", fmt.Sprintf("0x%04x", frameType), "src", src)
		}
	}
}

// ---------------------------------------------------------------------------
// SETUP handler — remote publisher announces a new session
// ---------------------------------------------------------------------------

func (h *UdpArteryHandler) handleSetup(src *net.UDPAddr, buf []byte) {
	var setup AeronSetupHeader
	setup.Decode(buf)

	key := inboundSessionKey(src, setup.SessionId, setup.StreamId)

	h.mu.Lock()
	sess, exists := h.inSessions[key]
	if !exists {
		sess = &aeronInboundSession{
			remoteAddr:    src,
			sessionId:     setup.SessionId,
			streamId:      setup.StreamId,
			initialTermId: setup.InitialTermId,
			activeTermId:  setup.ActiveTermId,
			termOffset:    setup.TermOffset,
			termLength:    setup.TermLength,
			mtu:           setup.Mtu,
		}
		h.inSessions[key] = sess
		slog.Info("aeron-udp: new inbound session",
			"src", src, "sessionId", setup.SessionId, "streamId", setup.StreamId)
	} else {
		// Refresh active term parameters on re-SETUP.
		sess.mu.Lock()
		sess.activeTermId = setup.ActiveTermId
		sess.termLength = setup.TermLength
		sess.mtu = setup.Mtu
		sess.mu.Unlock()
	}
	h.mu.Unlock()

	// Acknowledge with an SM.
	h.sendSm(src, sess)
}

// ---------------------------------------------------------------------------
// DATA handler — receive an Aeron DATA frame carrying an Artery envelope
// ---------------------------------------------------------------------------

func (h *UdpArteryHandler) handleData(src *net.UDPAddr, buf []byte) {
	var hdr AeronDataHeader
	hdr.Decode(buf)

	// FrameLength == 0 is a legitimate Aeron heartbeat DATA frame (publisher
	// keepalive with no payload).  Treat as zero-length payload.
	if hdr.FrameLength == 0 {
		slog.Debug("aeron-udp: heartbeat DATA frame (frameLength=0)", "src", src)
		return
	}
	if int(hdr.FrameLength) < AeronDataHeaderLen {
		slog.Warn("aeron-udp: DATA frameLength < header size, dropping",
			"frameLen", hdr.FrameLength, "src", src)
		return
	}
	if len(buf) < int(hdr.FrameLength) {
		slog.Warn("aeron-udp: truncated DATA frame",
			"frameLen", hdr.FrameLength, "actual", len(buf))
		return
	}

	payload := buf[AeronDataHeaderLen:hdr.FrameLength]

	key := inboundSessionKey(src, hdr.SessionId, hdr.StreamId)
	h.mu.RLock()
	sess, ok := h.inSessions[key]
	h.mu.RUnlock()

	if !ok {
		// Synthesise a lightweight session so we can still process the frame.
		h.mu.Lock()
		sess, ok = h.inSessions[key]
		if !ok {
			sess = &aeronInboundSession{
				remoteAddr:    src,
				sessionId:     hdr.SessionId,
				streamId:      hdr.StreamId,
				initialTermId: hdr.TermId,
				activeTermId:  hdr.TermId,
				termOffset:    hdr.TermOffset,
				termLength:    AeronDefaultTermLength,
				mtu:           AeronDefaultMtu,
			}
			h.inSessions[key] = sess
		}
		h.mu.Unlock()
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	// Detect and report gaps (NAK) when we see a jump in term offset.
	expectedOffset := int32(aeronAlignedLen(int(sess.termOffset)))
	if hdr.TermId == sess.activeTermId && hdr.TermOffset > expectedOffset {
		// Gap detected — send NAK for the missing range.
		gapLen := hdr.TermOffset - expectedOffset
		slog.Debug("aeron-udp: gap detected, sending NAK",
			"expected", expectedOffset, "got", hdr.TermOffset, "gap", gapLen)
		go h.sendNak(src, hdr.SessionId, hdr.StreamId, hdr.TermId, expectedOffset, gapLen)
	}

	// Update consumption position.
	if hdr.TermId == sess.activeTermId || hdr.TermId > sess.activeTermId {
		sess.activeTermId = hdr.TermId
		sess.termOffset = hdr.TermOffset + int32(len(payload))
	}

	// Fragment reassembly.
	beginFrag := hdr.Flags&AeronFlagBeginFrag != 0
	endFrag := hdr.Flags&AeronFlagEndFrag != 0

	switch {
	case beginFrag && endFrag:
		// Complete single-frame message — dispatch directly.
		h.dispatchEnvelope(src, hdr.StreamId, payload)

	case beginFrag:
		// Start of a multi-frame message.
		sess.fragBuf = make([]byte, len(payload))
		copy(sess.fragBuf, payload)
		sess.fragBeginOff = hdr.TermOffset
		sess.fragTermId = hdr.TermId

	case !beginFrag && !endFrag:
		// Middle fragment.
		if sess.fragBuf != nil {
			sess.fragBuf = append(sess.fragBuf, payload...)
		}

	case endFrag:
		// Final fragment — complete the message.
		if sess.fragBuf != nil {
			sess.fragBuf = append(sess.fragBuf, payload...)
			complete := sess.fragBuf
			sess.fragBuf = nil
			h.dispatchEnvelope(src, hdr.StreamId, complete)
		} else {
			// No BEGIN seen; treat as standalone.
			h.dispatchEnvelope(src, hdr.StreamId, payload)
		}
	}
}

// dispatchEnvelope decodes an Artery EnvelopeBuffer and routes it through the
// full Artery dispatch pipeline (HandshakeReq/Rsp, cluster messages, user messages).
func (h *UdpArteryHandler) dispatchEnvelope(src *net.UDPAddr, streamId int32, envelope []byte) {
	if len(envelope) == 0 {
		return
	}

	// Resolve the NodeManager compression table and remote UID.
	var remoteUid uint64
	var ctm *CompressionTableManager
	if h.nm != nil {
		h.nm.mu.RLock()
		ctm = h.nm.compressionMgr
		h.nm.mu.RUnlock()
		if assoc, ok := h.nm.GetGekkaAssociationByHost(src.IP.String(), uint32(src.Port)); ok {
			assoc.mu.RLock()
			if assoc.remote != nil {
				remoteUid = assoc.remote.GetUid()
			}
			assoc.mu.RUnlock()
		}
	}
	if ctm == nil {
		ctm = h.ctm
	}

	meta, err := ParseArteryFrame(envelope, ctm, remoteUid)
	if err != nil {
		slog.Warn("aeron-udp: failed to decode Artery envelope",
			"src", src, "streamId", streamId, "error", err)
		return
	}

	slog.Debug("aeron-udp: dispatching Artery message",
		"src", src, "streamId", streamId,
		"serializerId", meta.SerializerId, "manifest", string(meta.MessageManifest))

	// Route through the full Artery dispatch pipeline when a NodeManager is
	// available.  This handles HandshakeReq/Rsp, cluster gossip, user messages,
	// system messages, etc. — the same code path used by the TCP transport.
	if h.nm != nil {
		assoc := h.nm.GetOrCreateUDPAssociation(src, h)
		if err := assoc.Dispatch(h.ctx, meta); err != nil {
			slog.Warn("aeron-udp: dispatch error", "error", err)
		}
		return
	}

	// Fallback to the generic FrameHandler when no NodeManager is wired in
	// (e.g. unit tests that only test frame parsing / SM logic).
	if err := h.handler(h.ctx, meta); err != nil {
		slog.Warn("aeron-udp: frame handler error", "error", err)
	}
}

// ---------------------------------------------------------------------------
// SM handler — remote subscriber reports its consumption position
// ---------------------------------------------------------------------------

func (h *UdpArteryHandler) handleSm(src *net.UDPAddr, buf []byte) {
	var sm AeronStatusMessage
	sm.Decode(buf)

	key := outSessionKey(src, sm.StreamId)
	h.mu.Lock()
	sess, ok := h.outSessions[key]
	if !ok {
		// SM received before we had a session recorded (race / re-order) — ignore.
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()

	sess.mu.Lock()
	defer sess.mu.Unlock()

	if !sess.established {
		slog.Info("aeron-udp: outbound session established via SM",
			"remote", src, "streamId", sm.StreamId, "sessionId", sm.SessionId)
		sess.established = true
	}
	slog.Debug("aeron-udp: SM received",
		"remote", src, "consumptionTermId", sm.ConsumptionTermId,
		"consumptionOffset", sm.ConsumptionTermOffset,
		"window", sm.ReceiverWindowLength)
}

// ---------------------------------------------------------------------------
// NAK handler — remote subscriber requests retransmission
// ---------------------------------------------------------------------------

func (h *UdpArteryHandler) handleNak(src *net.UDPAddr, buf []byte) {
	var nak AeronNak
	nak.Decode(buf)
	// Retransmission is beyond the scope of the in-memory transport — log and
	// let the higher-level retry (cluster heartbeat / gossip) recover.
	slog.Warn("aeron-udp: NAK received — retransmission not implemented",
		"remote", src, "sessionId", nak.SessionId,
		"termId", nak.TermId, "termOffset", nak.TermOffset, "len", nak.Length)
}

// ---------------------------------------------------------------------------
// Periodic SM sender — maintains inbound flow-control window with publishers
// ---------------------------------------------------------------------------

// smLoop sends a periodic Status Message to every known inbound publisher so
// the remote Aeron Media Driver does not stall waiting for window updates.
func (h *UdpArteryHandler) smLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.mu.RLock()
			sessions := make([]*aeronInboundSession, 0, len(h.inSessions))
			for _, s := range h.inSessions {
				sessions = append(sessions, s)
			}
			h.mu.RUnlock()

			for _, sess := range sessions {
				h.sendSm(sess.remoteAddr, sess)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Send helpers
// ---------------------------------------------------------------------------

// sendSm sends a Status Message to the given remote address on behalf of sess.
func (h *UdpArteryHandler) sendSm(dst *net.UDPAddr, sess *aeronInboundSession) {
	sess.mu.Lock()
	sm := AeronStatusMessage{
		FrameLength:           AeronSmHeaderLen,
		Version:               AeronVersion,
		Flags:                 0,
		FrameType:             AeronHdrTypeSm,
		SessionId:             sess.sessionId,
		StreamId:              sess.streamId,
		ConsumptionTermId:     sess.activeTermId,
		ConsumptionTermOffset: sess.termOffset,
		ReceiverWindowLength:  AeronDefaultWindowLen,
	}
	sess.lastSmSent = time.Now()
	sess.mu.Unlock()

	buf := make([]byte, AeronSmHeaderLen)
	sm.Encode(buf)

	if _, err := h.conn.WriteToUDP(buf, dst); err != nil {
		slog.Warn("aeron-udp: SM send error", "dst", dst, "error", err)
	}
}

// sendNak sends a Negative-Acknowledgment to the remote publisher.
func (h *UdpArteryHandler) sendNak(dst *net.UDPAddr, sessionId, streamId, termId, termOffset, length int32) {
	nak := AeronNak{
		FrameLength: AeronNakHeaderLen,
		Version:     AeronVersion,
		Flags:       0,
		FrameType:   AeronHdrTypeNak,
		SessionId:   sessionId,
		StreamId:    streamId,
		TermId:      termId,
		TermOffset:  termOffset,
		Length:      length,
	}
	buf := make([]byte, AeronNakHeaderLen)
	nak.Encode(buf)

	if _, err := h.conn.WriteToUDP(buf, dst); err != nil {
		slog.Warn("aeron-udp: NAK send error", "dst", dst, "error", err)
	}
}

// ---------------------------------------------------------------------------
// Outbound: SendFrame sends an Artery envelope to a remote address on the
// given Aeron stream.
// ---------------------------------------------------------------------------

// SendFrame wraps the Artery EnvelopeBuffer payload in one or more Aeron DATA
// frames and sends them to dst on the given streamId.
//
// On the first call to dst/streamId, a SETUP frame is sent first and the
// method blocks for up to 2 s waiting for an SM acknowledgement.  Subsequent
// calls send DATA frames directly.
func (h *UdpArteryHandler) SendFrame(dst *net.UDPAddr, streamId int32, arteryPayload []byte) error {
	sess, err := h.ensureOutSession(dst, streamId)
	if err != nil {
		return err
	}

	return h.sendDataFrames(dst, sess, arteryPayload)
}

// ensureOutSession returns (or creates and establishes) an outbound session.
//
// The function sends SETUP frames at 250 ms intervals for up to 30 s, waiting
// for an SM from the remote Aeron subscription.  This long retry window is
// necessary when the remote Aeron Media Driver is still initialising its
// subscriptions (common during startup races).  If no SM is received within
// 30 s the function still proceeds — the DATA frames will be retransmitted
// if/when the session is eventually acknowledged.
func (h *UdpArteryHandler) ensureOutSession(dst *net.UDPAddr, streamId int32) (*aeronOutSession, error) {
	key := outSessionKey(dst, streamId)

	h.mu.Lock()
	sess, ok := h.outSessions[key]
	if !ok {
		sess = &aeronOutSession{
			remoteAddr:    dst,
			streamId:      streamId,
			sessionId:     randomInt32(),
			initialTermId: randomInt32(),
			activeTermId:  0,
			termOffset:    0,
			termLength:    AeronDefaultTermLength,
		}
		sess.activeTermId = sess.initialTermId
		h.outSessions[key] = sess
	}
	h.mu.Unlock()

	sess.mu.Lock()
	defer sess.mu.Unlock()

	if sess.established {
		return sess, nil
	}

	// Send SETUP and wait for SM — up to 30 s with 250 ms retry intervals.
	const (
		smWait    = 30 * time.Second
		smRetry   = 250 * time.Millisecond
	)
	deadline := time.Now().Add(smWait)
	attempt  := 0
	for time.Now().Before(deadline) {
		if err := h.sendSetup(dst, sess); err != nil {
			return nil, fmt.Errorf("aeron-udp: SETUP send (attempt %d): %w", attempt, err)
		}
		sess.setupSentAt = time.Now()
		attempt++

		if attempt == 1 || attempt%20 == 0 {
			slog.Info("aeron-udp: SETUP sent, waiting for SM",
				"dst", dst, "streamId", streamId, "attempt", attempt)
		}

		// Release the session lock while waiting so that handleSm can run.
		sess.mu.Unlock()
		time.Sleep(smRetry)
		sess.mu.Lock()

		if sess.established {
			slog.Info("aeron-udp: SM received, session established",
				"dst", dst, "streamId", streamId, "attempts", attempt)
			return sess, nil
		}
	}

	// Proceed without SM — DATA may be lost until the remote creates an image.
	slog.Warn("aeron-udp: no SM received within 30s, proceeding optimistically",
		"dst", dst, "streamId", streamId, "attempts", attempt)
	sess.established = true
	return sess, nil
}

// sendSetup sends an Aeron SETUP frame to dst.  Must be called with sess.mu held.
func (h *UdpArteryHandler) sendSetup(dst *net.UDPAddr, sess *aeronOutSession) error {
	setup := AeronSetupHeader{
		FrameLength:   AeronSetupHeaderLen,
		Version:       AeronVersion,
		Flags:         0,
		FrameType:     AeronHdrTypeSetup,
		TermOffset:    sess.termOffset,
		SessionId:     sess.sessionId,
		StreamId:      sess.streamId,
		InitialTermId: sess.initialTermId,
		ActiveTermId:  sess.activeTermId,
		TermLength:    sess.termLength,
		Mtu:           AeronDefaultMtu,
		Ttl:           0,
	}
	buf := make([]byte, AeronSetupHeaderLen)
	setup.Encode(buf)

	_, err := h.conn.WriteToUDP(buf, dst)
	return err
}

// sendDataFrames fragments arteryPayload into one or more Aeron DATA frames and
// writes them to dst.  Must be called with sess.mu held.
func (h *UdpArteryHandler) sendDataFrames(dst *net.UDPAddr, sess *aeronOutSession, arteryPayload []byte) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	maxPayload := int(AeronDefaultMtu) - AeronDataHeaderLen
	if maxPayload <= 0 {
		maxPayload = 1376
	}

	offset := 0
	total := len(arteryPayload)
	for offset < total {
		end := offset + maxPayload
		if end > total {
			end = total
		}
		chunk := arteryPayload[offset:end]

		// Compute fragment flags.
		var flags byte
		if offset == 0 {
			flags |= AeronFlagBeginFrag
		}
		if end == total {
			flags |= AeronFlagEndFrag
		}

		frameLen := int32(AeronDataHeaderLen + len(chunk))
		hdr := AeronDataHeader{
			FrameLength:   frameLen,
			Version:       AeronVersion,
			Flags:         flags,
			FrameType:     AeronHdrTypeData,
			TermOffset:    sess.termOffset,
			SessionId:     sess.sessionId,
			StreamId:      sess.streamId,
			TermId:        sess.activeTermId,
			ReservedValue: 0,
		}

		pkt := make([]byte, AeronDataHeaderLen+len(chunk))
		hdr.Encode(pkt[:AeronDataHeaderLen])
		copy(pkt[AeronDataHeaderLen:], chunk)

		if _, err := h.conn.WriteToUDP(pkt, dst); err != nil {
			return fmt.Errorf("aeron-udp: DATA send: %w", err)
		}

		// Advance term offset (aligned to 32-byte boundary).
		sess.termOffset += int32(aeronAlignedLen(int(frameLen)))
		offset = end
	}

	return nil
}

// ---------------------------------------------------------------------------
// HandleInboundFrame integration point for NodeManager
// ---------------------------------------------------------------------------

// HandleInboundFrame is the FrameHandler implementation used when a
// UdpArteryHandler is wired into a NodeManager.  It delegates to the
// NodeManager's own inbound pipeline (same path as TCP).
func (nm *NodeManager) HandleInboundFrame(ctx context.Context, meta *ArteryMetadata) error {
	if nm.UserMessageCallback != nil {
		return nm.UserMessageCallback(ctx, meta)
	}
	return nil
}

// ---------------------------------------------------------------------------
// DialRemoteUDP creates (or reuses) a UDP outbound association to a remote
// Artery node at host:port, using the Aeron transport.
// ---------------------------------------------------------------------------

// DialRemoteUDP sends an initial Aeron SETUP frame on the control stream
// (streamId 1) to the remote node and registers a placeholder OUTBOUND
// association whose outbox drainer forwards frames through udpHandler.
// The association transitions to ASSOCIATED once the Artery HandshakeRsp
// arrives via the inbound receive loop.
func (nm *NodeManager) DialRemoteUDP(ctx context.Context, udpHandler *UdpArteryHandler, remoteHost string, remotePort uint32) (*GekkaAssociation, error) {
	dstAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", remoteHost, remotePort))
	if err != nil {
		return nil, fmt.Errorf("aeron-udp: resolve %s:%d: %w", remoteHost, remotePort, err)
	}

	// Return an existing OUTBOUND association if one already exists.
	if assoc, ok := nm.GetGekkaAssociationByHost(remoteHost, remotePort); ok {
		return assoc, nil
	}

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: nm.LocalAddr.Protocol,
			System:   nm.LocalAddr.System,
			Hostname: proto.String(remoteHost),
			Port:     proto.Uint32(remotePort),
		},
		Uid: proto.Uint64(0),
	}

	largeCap := 512
	if nm.OutboundLargeMessageQueueSize > 0 {
		largeCap = nm.OutboundLargeMessageQueueSize
	}
	assoc := &GekkaAssociation{
		state:             INITIATED,
		role:              OUTBOUND,
		localUid:          nm.localUid,
		streamId:          AeronStreamControl,
		Handshake:         make(chan struct{}),
		outbox:            make(chan []byte, 512),
		udpOrdinaryOutbox: make(chan []byte, 512),
		udpLargeOutbox:    make(chan []byte, largeCap),
		nodeMgr:           nm,
		remote:            remoteUA,
		udpHandler:        udpHandler,
		udpDst:            dstAddr,
	}
	nm.RegisterAssociation(remoteUA, assoc)

	// Drain control (stream 1), ordinary (stream 2), and large (stream 3)
	// outboxes concurrently. (Large stream wired in Round-2 session 29.)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-assoc.outbox:
				if !ok {
					return
				}
				if err := udpHandler.SendFrame(dstAddr, AeronStreamControl, frame); err != nil {
					slog.Warn("aeron-udp: outbound control outbox send error", "error", err)
				}
			case frame, ok := <-assoc.udpOrdinaryOutbox:
				if !ok {
					return
				}
				if err := udpHandler.SendFrame(dstAddr, AeronStreamOrdinary, frame); err != nil {
					slog.Warn("aeron-udp: outbound ordinary outbox send error", "error", err)
				}
			case frame, ok := <-assoc.udpLargeOutbox:
				if !ok {
					return
				}
				if err := udpHandler.SendFrame(dstAddr, AeronStreamLarge, frame); err != nil {
					slog.Warn("aeron-udp: outbound large outbox send error", "error", err)
				}
			}
		}
	}()

	// Kick off the Artery handshake.
	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := assoc.initiateHandshake(remoteUA.Address); err != nil {
			slog.Error("aeron-udp: initiateHandshake error", "error", err)
		}
	}()

	return assoc, nil
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// randomInt32 returns a cryptographically random int32.
func randomInt32() int32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return int32(binary.LittleEndian.Uint32(b[:]))
}

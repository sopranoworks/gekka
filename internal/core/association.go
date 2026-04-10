/*
 * association.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// AssociationState represents the state of a connection to a remote node.
type AssociationState = actor.AssociationState

const (
	ASSOCIATED            = actor.ASSOCIATED
	QUARANTINED           = actor.QUARANTINED
	INITIATED             = actor.INITIATED
	WAITING_FOR_HANDSHAKE = actor.WAITING_FOR_HANDSHAKE
)

// AssociationRole indicates whether the connection was initiated locally or remotely.
type AssociationRole = actor.AssociationRole

const (
	INBOUND  = actor.INBOUND
	OUTBOUND = actor.OUTBOUND
)

// Association is a type alias for GekkaAssociation for backward compatibility.
type Association = GekkaAssociation

// GekkaAssociation tracks the state of a single connection.
type GekkaAssociation struct {
	mu        sync.RWMutex
	state     AssociationState
	role      AssociationRole
	conn      net.Conn
	remote    *gproto_remote.UniqueAddress
	lastSeen  time.Time
	nodeMgr   *NodeManager
	Handshake chan struct{} // signaled when associated
	nextSeq   uint64        // for user messages
	nextSeqNo uint64        // for system messages
	pending   [][]byte      // buffered frames during Handshake
	localUid  uint64
	outbox    chan []byte
	streamId  int32

	lastHeartbeatSentAt time.Time
	lastRTT             time.Duration

	// UDP transport fields — set when this association uses the Aeron-UDP
	// transport instead of a TCP connection.  When udpHandler is non-nil the
	// outbox drainer sends frames via udpHandler.SendFrame rather than
	// WriteFrame(conn, …).
	udpHandler        *UdpArteryHandler
	udpDst            *net.UDPAddr
	// udpOrdinaryOutbox carries user messages destined for Aeron stream 2
	// (OrdinaryStream).  Control/cluster/artery-internal messages continue to
	// use the primary outbox (stream 1).  Nil for TCP associations.
	udpOrdinaryOutbox chan []byte
}

var _ actor.RemoteAssociation = (*GekkaAssociation)(nil)

type NodeManager struct {
	mu                  sync.RWMutex
	LocalAddr           *gproto_remote.Address
	associations        map[string]*GekkaAssociation // key: host:port, or UID string
	localUid            uint64
	clusterMgr          *cluster.ClusterManager
	compressionMgr      *CompressionTableManager
	SerializerRegistry  *SerializationRegistry
	UserMessageCallback func(ctx context.Context, meta *ArteryMetadata) error
	SystemMessageCallback func(remote *gproto_remote.UniqueAddress, env *gproto_remote.SystemMessageEnvelope, msg *gproto_remote.SystemMessage) error

	pendingRepliesMu sync.RWMutex
	pendingReplies   map[string]chan *ArteryMetadata // keyed by temp actor path

	// quarantinedMu guards quarantinedUIDs.
	quarantinedMu sync.RWMutex
	// quarantinedUIDs is a permanent registry of remote node UIDs that have been
	// quarantined.  Unlike the associations map (which removes the entry upon
	// quarantine), this registry persists so that a restarting remote node with
	// the same UID cannot re-associate until the local process restarts.
	// Value is the UniqueAddress of the remote node at the time of quarantine.
	quarantinedUIDs map[uint64]*gproto_remote.UniqueAddress

	// mutedMu guards mutedNodes.
	mutedMu sync.RWMutex
	// mutedNodes is a set of "host:port" strings for which all outbound
	// frames are silently dropped and all inbound frames are ignored.
	// Populate via MuteNode / UnmuteNode to simulate a network partition
	// in tests without terminating the process.
	mutedNodes map[string]struct{}

	// pendingDials tracks in-progress DialRemote calls initiated by
	// handleHandshakeReq when Go is the seed and has no outbound to
	// a newly-connecting Pekko node. Keyed by "host:port". Protected
	// by mu (the NodeManager-level RWMutex).
	pendingDials map[string]bool

	// TLSConfig is the *tls.Config to use for outbound connections.
	// Nil means plain TCP (default).
	TLSConfig *tls.Config

	// NodeMetrics is the shared NodeMetrics instance (set by Cluster.Spawn).
	// Nil-safe: all callers check before touching.
	NodeMetrics *NodeMetrics

	// UDPHandler is set when the Aeron-UDP transport is active.  When non-nil,
	// DialRemote switches to the UDP path automatically.
	UDPHandler *UdpArteryHandler

	// udpSrcAssoc maps the physical UDP source address (e.g. "127.0.0.1:62159")
	// to an association.  This is separate from `associations` because
	// handleHandshakeReq overwrites assoc.remote with the canonical Akka
	// address (port 2561), making the port-based lookup in getAnyAssociationByHost
	// fail for subsequent frames from the ephemeral media-driver port.
	// Guarded by mu.
	udpSrcAssoc map[string]*GekkaAssociation
}

func NewNodeManager(local *gproto_remote.Address, uid uint64) *NodeManager {
	return &NodeManager{
		LocalAddr:          local,
		associations:       make(map[string]*GekkaAssociation),
		localUid:           uid,
		SerializerRegistry: NewSerializationRegistry(),
		pendingReplies:     make(map[string]chan *ArteryMetadata),
		mutedNodes:         make(map[string]struct{}),
		udpSrcAssoc:        make(map[string]*GekkaAssociation),
		quarantinedUIDs:    make(map[uint64]*gproto_remote.UniqueAddress),
	}
}

// IsQuarantined returns true when uid has been permanently quarantined.
// Thread-safe.
func (nm *NodeManager) IsQuarantined(uid uint64) bool {
	if uid == 0 {
		return false
	}
	nm.quarantinedMu.RLock()
	_, ok := nm.quarantinedUIDs[uid]
	nm.quarantinedMu.RUnlock()
	return ok
}

// RegisterQuarantinedUID permanently records remote as quarantined.
// Subsequent connection attempts from that UID will be rejected.
func (nm *NodeManager) RegisterQuarantinedUID(remote *gproto_remote.UniqueAddress) {
	if remote == nil {
		return
	}
	uid := remote.GetUid()
	if uid == 0 {
		return
	}
	nm.quarantinedMu.Lock()
	nm.quarantinedUIDs[uid] = remote
	nm.quarantinedMu.Unlock()
	slog.Warn("node manager: registered permanently quarantined UID", "uid", uid, "address", remote.GetAddress())
}

// QuarantinedUIDs returns a snapshot of all permanently quarantined UIDs.
// Used by diagnostics and tests.
func (nm *NodeManager) QuarantinedUIDs() []uint64 {
	nm.quarantinedMu.RLock()
	defer nm.quarantinedMu.RUnlock()
	out := make([]uint64, 0, len(nm.quarantinedUIDs))
	for uid := range nm.quarantinedUIDs {
		out = append(out, uid)
	}
	return out
}

// MuteNode silently drops all outbound frames to and all inbound frames from
// the node at host:port. This simulates a one-sided or full network partition
// without terminating either process. Safe to call concurrently.
func (nm *NodeManager) MuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	nm.mutedNodes[key] = struct{}{}
	nm.mutedMu.Unlock()
	slog.Info("node manager: muted node", "address", key)
}

// UnmuteNode reverses a previous MuteNode call. Safe to call even if the node
// was never muted.
func (nm *NodeManager) UnmuteNode(host string, port uint32) {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.Lock()
	delete(nm.mutedNodes, key)
	nm.mutedMu.Unlock()
	slog.Info("node manager: unmuted node", "address", key)
}

// isNodeMuted returns true when host:port has been muted.
func (nm *NodeManager) isNodeMuted(host string, port uint32) bool {
	key := fmt.Sprintf("%s:%d", host, port)
	nm.mutedMu.RLock()
	_, ok := nm.mutedNodes[key]
	nm.mutedMu.RUnlock()
	return ok
}

// RegisterPendingReply records a channel to receive a single reply addressed to path.
func (nm *NodeManager) RegisterPendingReply(path string, ch chan *ArteryMetadata) {
	nm.pendingRepliesMu.Lock()
	nm.pendingReplies[path] = ch
	nm.pendingRepliesMu.Unlock()
}

// UnregisterPendingReply removes the pending reply entry for path.
func (nm *NodeManager) UnregisterPendingReply(path string) {
	nm.pendingRepliesMu.Lock()
	delete(nm.pendingReplies, path)
	nm.pendingRepliesMu.Unlock()
}

// CountAssociations returns the number of Artery connections currently in
// ASSOCIATED state.  It is called by the monitoring server to populate the
// active_associations metric and to evaluate the /healthz readiness check.
func (nm *NodeManager) CountAssociations() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	count := 0
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		st := assoc.state
		assoc.mu.RUnlock()
		if st == ASSOCIATED {
			count++
		}
	}
	return count
}

// HasQuarantinedAssociation reports whether any known Artery association is in
// QUARANTINED state.  A quarantined association means the remote node restarted
// with a different UID — a network-split symptom that makes the local node
// unreliable for cluster operations.  Used by the /health/ready endpoint.
func (nm *NodeManager) HasQuarantinedAssociation() bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		st := assoc.state
		assoc.mu.RUnlock()
		if st == QUARANTINED {
			return true
		}
	}
	return false
}

// RoutePendingReply delivers meta to a waiting Ask call if path is registered.
// Returns true when a waiting caller was found and the message routed.
func (nm *NodeManager) RoutePendingReply(path string, meta *ArteryMetadata) bool {
	nm.pendingRepliesMu.RLock()
	ch, ok := nm.pendingReplies[path]
	nm.pendingRepliesMu.RUnlock()
	if !ok {
		return false
	}
	select {
	case ch <- meta:
	default:
		slog.Warn("node manager: Ask reply channel full, dropping", "path", path)
	}
	return true
}

var _ actor.RemoteMessagingProvider = (*NodeManager)(nil)

func (nm *NodeManager) LocalAddress() *gproto_remote.Address {
	return nm.LocalAddr
}

func (nm *NodeManager) GetAssociationByHost(host string, port uint32) (actor.RemoteAssociation, bool) {
	return nm.GetGekkaAssociationByHost(host, port)
}

// GetGekkaAssociationByHost returns a *GekkaAssociation for a remote node specified by host and port.
func (nm *NodeManager) GetGekkaAssociationByHost(host string, port uint32) (*GekkaAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	// Prioritize Control Stream (1) for outbound delivery.
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		role := assoc.role
		state := assoc.state
		streamId := assoc.streamId
		assoc.mu.RUnlock()

		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			if role == OUTBOUND && (state == ASSOCIATED || state == INITIATED || state == WAITING_FOR_HANDSHAKE) {
				if streamId == 1 {
					return assoc, true
				}
			}
		}
	}

	// Fallback to any outbound if control not found
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		role := assoc.role
		state := assoc.state
		assoc.mu.RUnlock()

		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			if role == OUTBOUND && (state == ASSOCIATED || state == INITIATED || state == WAITING_FOR_HANDSHAKE) {
				return assoc, true
			}
		}
	}
	return nil, false
}
func (nm *NodeManager) DialRemote(ctx context.Context, target *gproto_remote.Address) (actor.RemoteAssociation, error) {
	// Aeron-UDP path: delegate to the UDP handler when configured.
	if nm.UDPHandler != nil {
		return nm.DialRemoteUDP(ctx, nm.UDPHandler, target.GetHostname(), target.GetPort())
	}

	addrStr := fmt.Sprintf("%s:%d", target.GetHostname(), target.GetPort())

	client, err := NewTcpClient(TcpClientConfig{
		Addr: addrStr,
		Handler: func(ctx context.Context, conn net.Conn) error {
			return nm.ProcessConnection(ctx, conn, OUTBOUND, target, 1) // Default to Control stream for outbound
		},
		TLSConfig: nm.TLSConfig,
	})
	if err != nil {
		return nil, err
	}

	// Start connection in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Connect(ctx)
	}()

	// Wait for the association to reach ASSOCIATED state (handshake complete)
	// before returning. Returning a pre-ASSOCIATED association causes callers
	// to write frames that Pekko's Artery discards during its handshake phase.
	timeout := time.After(10 * time.Second)
	for {
		select {
		case err := <-errChan:
			return nil, err
		case <-timeout:
			// Fall back to returning whatever we have (even pre-ASSOCIATED)
			// so the caller can at least queue messages.
			if assoc, ok := nm.GetAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				return assoc, nil
			}
			return nil, fmt.Errorf("dial timeout")
		case <-time.After(100 * time.Millisecond):
			if ga, ok := nm.GetGekkaAssociationByHost(target.GetHostname(), target.GetPort()); ok {
				ga.mu.RLock()
				st := ga.state
				ga.mu.RUnlock()
				if st == ASSOCIATED {
					return ga, nil
				}
			}
		}
	}
}

func (nm *NodeManager) Serializer(id int32) (actor.RemoteSerializer, error) {
	s, err := nm.SerializerRegistry.GetSerializer(id)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (nm *NodeManager) Metrics() actor.RemoteMetrics {
	return nm.NodeMetrics
}

func (nm *NodeManager) SetClusterManager(cm *cluster.ClusterManager) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.clusterMgr = cm
}

func (nm *NodeManager) SetCompressionManager(ctm *CompressionTableManager) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.compressionMgr = ctm
}

// MessageContext carries metadata about an incoming Artery message.
type MessageContext struct {
	Sender     *gproto_remote.UniqueAddress
	Recipient  *gproto_remote.Address
	Serializer int32
	Manifest   string
	SeqNo      uint64
	AckReplyTo *gproto_remote.UniqueAddress
}

// GetAssociation looks up an existing association by unique address and streamId.
func (nm *NodeManager) GetAssociation(remote *gproto_remote.UniqueAddress, streamId int32) (*GekkaAssociation, bool) {
	if remote == nil || remote.Address == nil {
		return nil, false
	}
	addr := remote.GetAddress()
	key := fmt.Sprintf("%s:%d-%d-s%d", addr.GetHostname(), addr.GetPort(), remote.GetUid(), streamId)
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	assoc, ok := nm.associations[key]
	return assoc, ok
}

// GetAssociationByRemote returns an association for a specific remote node by its UID string.
func (nm *NodeManager) GetAssociationByRemote(remoteID string, streamId int32) (*GekkaAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	// The key format in RegisterAssociation is host:port-uid-sStreamId
	// We scan for the -uid-sStreamId suffix.
	suffix := fmt.Sprintf("-%s-s%d", remoteID, streamId)
	for k, assoc := range nm.associations {
		if strings.HasSuffix(k, suffix) {
			return assoc, true
		}
	}
	return nil, false
}

// UniqueAddressToString converts a UniqueAddress to a stable string identifier for maps.
func UniqueAddressToString(ua *gproto_remote.UniqueAddress) string {
	if ua == nil {
		return ""
	}
	return fmt.Sprintf("%d", ua.GetUid())
}

// RegisterAssociation stores an association in the registry.
func (nm *NodeManager) RegisterAssociation(remote *gproto_remote.UniqueAddress, assoc *GekkaAssociation) {
	if remote == nil || remote.Address == nil {
		return
	}
	addr := remote.GetAddress()
	newKey := fmt.Sprintf("%s:%d-%d-s%d", addr.GetHostname(), addr.GetPort(), remote.GetUid(), assoc.streamId)

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// UID Check: Check if there's an existing association for the same Host:Port but different UID.
	// UID=0 (newKey) is an early placeholder — the confirmed remote UID is not yet known, so
	// we cannot make a node-restart determination and must skip the quarantine check entirely.
	hostPortKey := fmt.Sprintf("%s:%d-", addr.GetHostname(), addr.GetPort())
	if !strings.Contains(newKey, "-0-s") {
		for k, existing := range nm.associations {
			if strings.HasPrefix(k, hostPortKey) && !strings.Contains(k, fmt.Sprintf("-%d-s", remote.GetUid())) {
				// Skip early registrations (UID=0 placeholders).
				if strings.Contains(k, "-0-s") {
					slog.Debug("node manager: existing early registration found for host-port", "key", k, "hostPort", hostPortKey)
					continue
				}

				slog.Warn("node manager: detected node restart, quarantining old association", "hostPort", hostPortKey, "oldKey", k)
				existing.mu.Lock()
				existingRemote := existing.remote
				existing.state = QUARANTINED
				if existing.conn != nil {
					existing.conn.Close()
				}
				existing.mu.Unlock()
				delete(nm.associations, k)
				// Register the old UID permanently so the remote cannot re-associate
				// with that UID after we drop the connection.
				if existingRemote != nil {
					nm.quarantinedMu.Lock()
					nm.quarantinedUIDs[existingRemote.GetUid()] = existingRemote
					nm.quarantinedMu.Unlock()
				}
				// Notify the remote that we have quarantined it, using the new
				// association's outbox so the frame reaches the remote over the
				// live transport.
				go existing.SendQuarantined(existingRemote)
			}
		}
	}

	// Never let an INBOUND association overwrite an existing OUTBOUND one for the same key.
	assoc.mu.RLock()
	incomingIsInbound := assoc.role == INBOUND
	assoc.mu.RUnlock()
	if incomingIsInbound {
		if existing, found := nm.associations[newKey]; found {
			existing.mu.RLock()
			existingIsOutbound := existing.role == OUTBOUND
			existing.mu.RUnlock()
			if existingIsOutbound {
				slog.Debug("node manager: INBOUND skipping registration — OUTBOUND already exists", "key", newKey)
				return
			}
		}
	}

	nm.associations[newKey] = assoc
	slog.Debug("node manager: registered association", "key", newKey)
}

// ProcessConnection is the unified entry point for both inbound and outbound connections.
func (nm *NodeManager) ProcessConnection(ctx context.Context, conn net.Conn, role AssociationRole, remote *gproto_remote.Address, streamId int32) error {
	assocCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	protocol := "akka" // default
	if nm.LocalAddr != nil && nm.LocalAddr.GetProtocol() != "" {
		protocol = nm.LocalAddr.GetProtocol()
	}

	if role == INBOUND {
		// Read first 3 bytes to decide preamble format
		magic3 := make([]byte, 3)
		if _, err := io.ReadFull(conn, magic3); err != nil {
			return fmt.Errorf("failed to read artery magic: %w", err)
		}

		if string(magic3) == "ART" {
			// Artery 2.0 (Pekko): ART (3) + version (1) + streamId (1) = 5 bytes total
			rest := make([]byte, 2)
			if _, err := io.ReadFull(conn, rest); err != nil {
				return fmt.Errorf("failed to read Pekko preamble rest: %w", err)
			}
			streamId = int32(rest[1])
			protocol = "pekko"
			slog.Info("artery: INBOUND Pekko preamble read", "streamId", streamId)
		} else {
			// Artery 1.0 (Akka): AKKA (4) + streamId (1) = 5 bytes total
			// magic3 already consumed "AKK"
			next := make([]byte, 2) // 'A' + streamId byte
			if _, err := io.ReadFull(conn, next); err != nil {
				return fmt.Errorf("failed to read Akka magic rest: %w", err)
			}
			if magic3[0] != 'A' || magic3[1] != 'K' || magic3[2] != 'K' || next[0] != 'A' {
				return fmt.Errorf("invalid artery magic: %q", string(magic3)+string(next))
			}
			streamId = int32(next[1])
			protocol = "akka"
			slog.Info("artery: INBOUND Akka preamble read", "streamId", streamId)
		}
	}

	assoc := &GekkaAssociation{
		state:     INITIATED,
		role:      role,
		conn:      conn,
		nodeMgr:   nm,
		lastSeen:  time.Now(),
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 100),
		remote:    &gproto_remote.UniqueAddress{Address: remote, Uid: proto.Uint64(0)},
		streamId:  streamId,
	}
	// Register early so handleHandshakeRsp can find it
	if remote != nil {
		nm.RegisterAssociation(assoc.remote, assoc)
	}

	// Start background write loop.
	// Uses the outer ctx (node lifetime), NOT assocCtx, so that the write loop
	// keeps draining the outbox even after the read loop exits.  In Artery-TCP
	// the outbound TCP stream is unidirectional: Pekko half-closes its write end
	// immediately, causing the read loop to get EOF and return, but writes in the
	// opposite direction are still valid and must continue.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-assoc.outbox:
				if !ok {
					return
				}
				// Check mute state before writing (catches buffered frames
				// queued before MuteNode was called). Use assoc.remote under
				// read lock since it's updated after the handshake.
				assoc.mu.RLock()
				remoteAddr := assoc.remote.GetAddress()
				assoc.mu.RUnlock()
				if remoteAddr != nil && nm.isNodeMuted(remoteAddr.GetHostname(), remoteAddr.GetPort()) {
					continue
				}
				slog.Debug("artery: sending frame", "total_bytes", len(msg))
				// UDP path: wrap in Aeron DATA frame and send via UDP socket.
				assoc.mu.RLock()
				udpH := assoc.udpHandler
				udpDst := assoc.udpDst
				assoc.mu.RUnlock()
				if udpH != nil && udpDst != nil {
					if err := udpH.SendFrame(udpDst, AeronStreamControl, msg); err != nil {
						slog.Warn("aeron-udp: outbox send error", "error", err)
					}
					continue
				}
				// TCP path: prepend 4-byte length and write to the connection.
				if err := WriteFrame(assoc.conn, msg); err != nil {
					slog.Error("artery: write error", "error", err)
					// Close so the read loop also exits cleanly.
					_ = assoc.conn.Close()
					return
				}
			}
		}
	}()

	if role == OUTBOUND && remote != nil {
		go func() {
			// Give handler a moment to start and send magic header
			time.Sleep(200 * time.Millisecond)
			if err := assoc.initiateHandshake(remote); err != nil {
				slog.Error("artery: initiateHandshake error", "error", err)
			}

			// Start heartbeat loop.
			// Guard: only send heartbeats after the handshake is fully complete
			// (state == ASSOCIATED) AND the remote UID is known (non-zero).
			// Sending "m" frames before these conditions are met is a protocol
			// violation: Akka's ControlMessageObserver associates the heartbeat
			// with a specific remote UID, and an unknown (zero) UID causes it to
			// drop or misroute the frame.
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-assocCtx.Done():
					return
				case <-ticker.C:
					assoc.mu.RLock()
					isAssociated := assoc.state == ASSOCIATED
					remoteUID := uint64(0)
					if assoc.remote != nil {
						remoteUID = assoc.remote.GetUid()
					}
					assoc.mu.RUnlock()
					if isAssociated && remoteUID != 0 {
						if err := SendArteryHeartbeat(assoc); err != nil {
							slog.Debug("artery: failed to send heartbeat", "error", err)
						}
					}
				}
			}
		}()
	}

	return assoc.Process(assocCtx, protocol)
}

func (assoc *GekkaAssociation) LocalUid() uint64 {
	return assoc.localUid
}

func (assoc *GekkaAssociation) Outbox() chan []byte {
	return assoc.outbox
}

func (assoc *GekkaAssociation) initiateHandshake(to *gproto_remote.Address) error {
	assoc.mu.Lock()
	assoc.state = WAITING_FOR_HANDSHAKE
	uid := assoc.localUid
	assoc.mu.Unlock()

	// Correctly initialize HandshakeReq using pointer types from proto package
	req := &gproto_remote.HandshakeReq{
		From: &gproto_remote.UniqueAddress{
			Address: assoc.nodeMgr.LocalAddr,
			Uid:     proto.Uint64(uid),
		},
		To: to,
	}
	// Pekko ArteryMessageSerializer (17) uses "d" for HandshakeReq.
	// We use 0 (UnknownUid) because we don't know the remote UID yet.

	// For UDP associations (conn == nil) build the frame and route it through
	// the outbox so the correct transport layer (Aeron DATA frame) is used.
	assoc.mu.RLock()
	isUDP := assoc.udpHandler != nil
	assoc.mu.RUnlock()

	if isUDP {
		senderPath := fmt.Sprintf("%s://%s@%s:%d",
			req.From.GetAddress().GetProtocol(),
			req.From.GetAddress().GetSystem(),
			req.From.GetAddress().GetHostname(),
			req.From.GetAddress().GetPort())
		msgPayload, err := proto.Marshal(req)
		if err != nil {
			return fmt.Errorf("artery: marshal HandshakeReq: %w", err)
		}
		frame, err := BuildArteryFrame(int64(uid), actor.ArteryInternalSerializerID, senderPath, "", "d", msgPayload, true)
		if err != nil {
			return err
		}
		select {
		case assoc.outbox <- frame:
			return nil
		default:
			return fmt.Errorf("artery: outbox full during HandshakeReq (UDP)")
		}
	}

	return SendArteryMessageWithAck(assoc.conn, int64(uid), actor.ArteryInternalSerializerID, "d", req, req.From, true)
}

func (assoc *GekkaAssociation) Process(ctx context.Context, protocol string) error {
	dispatch := func(ctx context.Context, meta *ArteryMetadata) error {
		return assoc.dispatch(ctx, meta)
	}
	remoteUid := uint64(0)
	if assoc.remote != nil {
		remoteUid = assoc.remote.GetUid()
	}
	if assoc.role == OUTBOUND {
		return TcpArteryOutboundHandler(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId, protocol)
	}
	return TcpArteryHandlerWithCallback(ctx, assoc.conn, dispatch, assoc.nodeMgr.compressionMgr, remoteUid, assoc.streamId)
}

func (assoc *GekkaAssociation) dispatch(ctx context.Context, meta *ArteryMetadata) error {
	// Drop all inbound frames from a muted node (simulates network partition).
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	assoc.mu.Lock()
	assoc.lastSeen = time.Now()
	assoc.mu.Unlock()

	if meta.SeqNo != 0 && meta.AckReplyTo != nil {
		if err := assoc.sendSystemAck(meta.SeqNo, meta.AckReplyTo); err != nil {
			slog.Debug("artery: failed to send ACK", "seq", meta.SeqNo, "error", err)
		}
	}

	switch meta.SerializerId {
	case actor.ArteryInternalSerializerID:
		manifest := string(meta.MessageManifest)
		if manifest == "SystemMessage" {
			return assoc.handleSystemMessage(meta)
		}
		return assoc.handleControlMessage(ctx, meta)

	case 6: // MessageContainerSerializer
		env := &gproto_remote.SelectionEnvelope{}
		if err := proto.Unmarshal(meta.Payload, env); err != nil {
			return fmt.Errorf("failed to unmarshal SelectionEnvelope: %w", err)
		}

		// 1. Resolve target path from Pattern
		path := "/"
		for _, e := range env.Pattern {
			if e.GetType() == gproto_remote.PatternType_PARENT {
				lastSlash := strings.LastIndex(strings.TrimSuffix(path, "/"), "/")
				if lastSlash != -1 {
					path = path[:lastSlash+1]
				}
			} else {
				if !strings.HasSuffix(path, "/") {
					path += "/"
				}
				path += e.GetMatcher()
			}
		}

		// 2. Route cluster messages directly — ClusterSerializer (ID=5) is not
		// registered in SerializerRegistry, so calling DeserializePayload would fail.
		if env.GetSerializerId() == actor.ClusterSerializerID {
			if assoc.nodeMgr.clusterMgr != nil {
				assoc.mu.RLock()
				remote := assoc.remote
				assoc.mu.RUnlock()
				return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(ctx, env.GetEnclosedMessage(), string(env.GetMessageManifest()), ToClusterUniqueAddress(remote))
			}
			return nil
		}

		// 2b. Handle Identify (MiscMessageSerializer, sid=16, manifest "A") —
		// reply with ActorIdentity so Pekko's actorSelection(...).resolveOne() works.
		// Only respond to manifest "A" (Identify); other MiscMessage types are dropped.
		if env.GetSerializerId() == MiscMessageSerializerID && string(env.GetMessageManifest()) == "A" {
			meta.Recipient = &gproto_remote.ActorRefData{Path: proto.String(path)}
			assoc.handleIdentify(meta, env)
			return nil
		}

		// 3. Deserialize inner message for non-cluster payloads
		innerMsg, err := assoc.nodeMgr.SerializerRegistry.DeserializePayload(env.GetSerializerId(), string(env.GetMessageManifest()), env.GetEnclosedMessage())
		if err != nil {
			// Non-fatal: unknown serializer or manifest collision should not
			// kill the TCP connection.  Log and drop the message.
			slog.Debug("artery: selection failed to deserialize inner message (dropping)", "error", err)
			return nil
		}

		// 4. Update meta and route
		meta.DeserializedMessage = innerMsg
		meta.Recipient = &gproto_remote.ActorRefData{Path: proto.String(path)}

		return assoc.handleUserMessage(meta)

	case actor.ClusterSerializerID:
		if assoc.nodeMgr.clusterMgr != nil {
			assoc.mu.RLock()
			remote := assoc.remote
			assoc.mu.RUnlock()
			return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(ctx, meta.Payload, string(meta.MessageManifest), ToClusterUniqueAddress(remote))
		}
		return nil

	default:
		return assoc.handleUserMessage(meta)
	}
}

func (assoc *GekkaAssociation) sendSystemAck(seq uint64, to *gproto_remote.UniqueAddress) error {
	ack := &gproto_remote.SystemMessageDeliveryAck{
		SeqNo: proto.Uint64(seq),
		From: &gproto_remote.UniqueAddress{
			Address: assoc.nodeMgr.LocalAddr,
			Uid:     proto.Uint64(assoc.localUid),
		},
	}
	// "h" is the ArteryMessageSerializer short manifest for SystemMessageDeliveryAck.
	// Using the long class name causes Akka's ControlMessageObserver to skip the frame
	// and pass it to MessageDispatcher.dispatch with an empty recipient → OptionVal.None.get.
	return SendArteryMessageWithAck(assoc.conn, int64(assoc.localUid), actor.ArteryInternalSerializerID, "h", ack, to, true)
}

func (assoc *GekkaAssociation) handleSystemMessage(meta *ArteryMetadata) error {
	// The Artery payload for manifest "SystemMessage" is a SystemMessageEnvelope
	// which carries the SeqNo, AckReplyTo, and the inner SystemMessage bytes.
	env := &gproto_remote.SystemMessageEnvelope{}
	if err := proto.Unmarshal(meta.Payload, env); err != nil {
		return fmt.Errorf("failed to unmarshal SystemMessageEnvelope: %w", err)
	}
	if env.GetSeqNo() != 0 && env.AckReplyTo != nil {
		if err := assoc.sendSystemAck(env.GetSeqNo(), env.AckReplyTo); err != nil {
			slog.Debug("artery: failed to send ACK", "seq", env.GetSeqNo(), "error", err)
		}
	}
	sm := &gproto_remote.SystemMessage{}
	if err := proto.Unmarshal(env.Message, sm); err != nil {
		return fmt.Errorf("failed to unmarshal inner SystemMessage: %w", err)
	}
	slog.Debug("artery: received system message", "type", sm.GetType())

	if assoc.nodeMgr.SystemMessageCallback != nil {
		return assoc.nodeMgr.SystemMessageCallback(assoc.remote, env, sm)
	}
	return nil
}

func (assoc *GekkaAssociation) handleUserMessage(meta *ArteryMetadata) error {
	// Count every incoming user message (cluster-internal messages never
	// reach this handler — they go to handleControlMessage/cluster.ClusterManager).
	if assoc.nodeMgr.NodeMetrics != nil {
		assoc.nodeMgr.NodeMetrics.MessagesReceived.Add(1)
		assoc.nodeMgr.NodeMetrics.BytesReceived.Add(int64(len(meta.Payload)))
	}

	if assoc.nodeMgr.SerializerRegistry != nil {
		obj, err := assoc.nodeMgr.SerializerRegistry.DeserializePayload(meta.SerializerId, string(meta.MessageManifest), meta.Payload)
		if err == nil {
			meta.DeserializedMessage = obj
		} else {
			slog.Debug("artery: failed to deserialize payload",
				"serializerId", meta.SerializerId,
				"manifest", meta.MessageManifest,
				"error", err)
		}
	}

	// Route to a pending Ask call if the recipient path is registered.
	if meta.Recipient != nil {
		recipientPath := meta.Recipient.GetPath()
		// If the recipient is a full URI, extract the path segment for pending-reply lookup.
		if strings.Contains(recipientPath, "://") {
			// Find the start of the path part (after the authority)
			// URI format: scheme://system@host:port/path
			if firstSlash := strings.Index(recipientPath[strings.Index(recipientPath, "://")+3:], "/"); firstSlash != -1 {
				recipientPath = recipientPath[strings.Index(recipientPath, "://")+3+firstSlash:]
			}
		}
		if recipientPath != "" && assoc.nodeMgr.RoutePendingReply(recipientPath, meta) {
			return nil
		}
	}

	if assoc.nodeMgr.UserMessageCallback != nil {
		return assoc.nodeMgr.UserMessageCallback(context.Background(), meta)
	}
	return nil
}

// SendWithSender delivers a message to recipient over this association using
// senderPath as the Artery sender actor path (required by the Ask pattern).
func (assoc *GekkaAssociation) SendWithSender(recipient, senderPath string, payload []byte, serializerId int32, manifest string) error {
	assoc.mu.RLock()
	st := assoc.state
	assoc.mu.RUnlock()

	if st == QUARANTINED {
		return fmt.Errorf("cannot send to quarantined node")
	}

	// Drop outbound frames when the target node is muted.
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	frame, err := BuildArteryFrame(int64(assoc.localUid), serializerId, senderPath, recipient, manifest, payload, false)
	if err != nil {
		return err
	}
	slog.Debug("artery: SendWithSender frame", "total_bytes", len(frame), "sender", senderPath, "recipient", recipient)

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	// User messages go on Aeron stream 2 (Ordinary) when a separate outbox is
	// available (UDP transport).  Control/cluster messages stay on stream 1.
	outbox := assoc.udpOrdinaryOutboxFor(serializerId)
	select {
	case outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

func (assoc *GekkaAssociation) GetState() AssociationState {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.state
}

// SendQuarantined enqueues a "Quarantined" control frame to notify the remote
// node that we have permanently quarantined it.  The frame is sent on the
// outbox so it piggybacks on the existing TCP/UDP transport without blocking
// the caller.
func (assoc *GekkaAssociation) SendQuarantined(to *gproto_remote.UniqueAddress) {
	nm := assoc.nodeMgr
	if nm == nil || nm.LocalAddr == nil {
		return
	}
	localUA := &gproto_remote.UniqueAddress{
		Address: nm.LocalAddr,
		Uid:     proto.Uint64(assoc.localUid),
	}
	msg := &gproto_remote.Quarantined{
		From: localUA,
		To:   to,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		slog.Warn("artery: failed to marshal Quarantined", "error", err)
		return
	}
	frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "Quarantined", payload, true)
	if err != nil {
		slog.Warn("artery: failed to build Quarantined frame", "error", err)
		return
	}
	select {
	case assoc.outbox <- frame:
		slog.Info("artery: sent Quarantined frame", "to", to)
	default:
		slog.Warn("artery: outbox full, Quarantined frame dropped", "to", to)
	}
}

func (assoc *GekkaAssociation) GetRTT() time.Duration {
	assoc.mu.RLock()
	defer assoc.mu.RUnlock()
	return assoc.lastRTT
}

func (assoc *GekkaAssociation) NextSeq() uint64 {
	assoc.mu.Lock()
	defer assoc.mu.Unlock()
	assoc.nextSeq++
	return assoc.nextSeq
}

func (assoc *GekkaAssociation) NextSeqNo() uint64 {
	assoc.mu.Lock()
	defer assoc.mu.Unlock()
	assoc.nextSeqNo++
	return assoc.nextSeqNo
}

func (assoc *GekkaAssociation) Send(recipient string, payload []byte, serializerId int32, manifest string) error {
	assoc.mu.RLock()
	st := assoc.state
	assoc.mu.RUnlock()

	if st == QUARANTINED {
		return fmt.Errorf("cannot send to quarantined node")
	}

	// Drop outbound frames when the target node is muted.
	if assoc.nodeMgr != nil && assoc.remote != nil {
		a := assoc.remote.GetAddress()
		if assoc.nodeMgr.isNodeMuted(a.GetHostname(), a.GetPort()) {
			return nil
		}
	}

	// Artery messages from Gekka's cluster manager should appear to come from
	// the cluster daemon. Use a full URL so Akka/Pekko can resolve the sender.
	sender := fmt.Sprintf("%s://%s@%s:%d/system/cluster/core/daemon",
		assoc.nodeMgr.LocalAddr.GetProtocol(),
		assoc.nodeMgr.LocalAddr.GetSystem(),
		assoc.nodeMgr.LocalAddr.GetHostname(),
		assoc.nodeMgr.LocalAddr.GetPort())

	frame, err := BuildArteryFrame(int64(assoc.localUid), serializerId, sender, recipient, manifest, payload, false)
	if err != nil {
		return err
	}
	slog.Debug("artery: sending frame", "total_bytes", len(frame), "remote_uid", assoc.remote.GetUid(), "serializerId", serializerId, "manifest", manifest)

	assoc.mu.Lock()
	defer assoc.mu.Unlock()

	if assoc.state != ASSOCIATED {
		slog.Debug("artery: buffering message", "state", assoc.state)
		assoc.pending = append(assoc.pending, frame)
		return nil
	}

	outbox := assoc.udpOrdinaryOutboxFor(serializerId)
	select {
	case outbox <- frame:
		return nil
	default:
		return fmt.Errorf("association outbox full")
	}
}

// udpOrdinaryOutboxFor returns the appropriate outbox channel for serializerId.
// User messages (non-cluster, non-artery-internal) use the Aeron ordinary stream
// (stream 2) when available.  Everything else uses the primary outbox (stream 1).
// For TCP associations udpOrdinaryOutbox is nil, so the primary outbox is always used.
func (assoc *GekkaAssociation) udpOrdinaryOutboxFor(serializerId int32) chan []byte {
	if assoc.udpOrdinaryOutbox != nil &&
		serializerId != ClusterSerializerID &&
		serializerId != ArteryInternalSerializerID {
		return assoc.udpOrdinaryOutbox
	}
	return assoc.outbox
}

func (assoc *GekkaAssociation) handleControlMessage(ctx context.Context, meta *ArteryMetadata) error {
	manifest := string(meta.MessageManifest)
	slog.Debug("artery: handling control message", "manifest", manifest)
	switch manifest {
	case "d": // HandshakeReq
		req := &gproto_remote.HandshakeReq{}
		if err := proto.Unmarshal(meta.Payload, req); err != nil {
			return err
		}
		return assoc.handleHandshakeReq(req)

	case "e": // HandshakeRsp
		mwa := &gproto_remote.MessageWithAddress{}
		if err := proto.Unmarshal(meta.Payload, mwa); err != nil {
			return err
		}
		return assoc.handleHandshakeRsp(mwa)

	case "m": // ArteryHeartbeat — Pekko ArteryMessageSerializer manifest
		// ArteryHeartbeat is a Scala singleton with an empty payload.
		// Reply immediately with ArteryHeartbeatRsp containing our local UID so
		// Pekko's RemoteWatcher does not mark the Go node as unreachable.
		slog.Debug("artery: ArteryHeartbeat received — replying with ArteryHeartbeatRsp", "uid", assoc.localUid)
		rsp := &gproto_remote.ArteryHeartbeatRsp{Uid: proto.Uint64(assoc.localUid)}
		payload, err := proto.Marshal(rsp)
		if err != nil {
			return fmt.Errorf("failed to marshal ArteryHeartbeatRsp: %w", err)
		}
		frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "n", payload, true)
		if err != nil {
			return fmt.Errorf("failed to build ArteryHeartbeatRsp frame: %w", err)
		}
		select {
		case assoc.outbox <- frame:
		default:
			slog.Debug("artery: ArteryHeartbeatRsp outbox full, dropping response")
		}
		return nil

	case "n": // ArteryHeartbeatRsp — Pekko's reply to a heartbeat we sent
		hb := &gproto_remote.ArteryHeartbeatRsp{}
		if err := proto.Unmarshal(meta.Payload, hb); err != nil {
			return err
		}
		assoc.mu.Lock()
		if !assoc.lastHeartbeatSentAt.IsZero() {
			assoc.lastRTT = time.Since(assoc.lastHeartbeatSentAt)
			slog.Debug("artery: ArteryHeartbeatRsp received", "uid", hb.GetUid(), "rtt", assoc.lastRTT)
		} else {
			slog.Debug("artery: ArteryHeartbeatRsp received", "uid", hb.GetUid())
		}
		assoc.mu.Unlock()
		return nil

	case "ActorRefCompressionAdvertisement", "ClassManifestCompressionAdvertisement":
		if assoc.nodeMgr.compressionMgr != nil {
			adv := &gproto_remote.CompressionTableAdvertisement{}
			if err := proto.Unmarshal(meta.Payload, adv); err != nil {
				return err
			}
			isActorRef := manifest == "ActorRefCompressionAdvertisement"
			// Get local address from NodeManager to use in the Ack
			localUA := &gproto_remote.UniqueAddress{
				Address: assoc.nodeMgr.LocalAddr,
				Uid:     proto.Uint64(assoc.nodeMgr.localUid),
			}
			return assoc.nodeMgr.compressionMgr.HandleAdvertisement(ctx, adv, isActorRef, localUA)
		}
		return nil

	case "Quarantined":
		// Remote has detected a UID conflict and is notifying us. Quarantine the association.
		quar := &gproto_remote.Quarantined{}
		if err := proto.Unmarshal(meta.Payload, quar); err != nil {
			return err
		}
		slog.Warn("artery: received Quarantined", "from", quar.From, "to", quar.To)
		// Register the sender UID permanently so we also refuse future re-association from them.
		if assoc.nodeMgr != nil {
			assoc.nodeMgr.RegisterQuarantinedUID(quar.From)
		}
		assoc.mu.Lock()
		assoc.state = QUARANTINED
		if assoc.conn != nil {
			assoc.conn.Close()
		}
		assoc.mu.Unlock()
		return nil

	case "ActorRefCompressionAdvertisementAck", "ClassManifestCompressionAdvertisementAck":
		// We log the ack, but we don't block on receiving it yet.
		// In a full implementation, we'd wait for this before transitioning to using the compressed IDs.
		ack := &gproto_remote.CompressionTableAdvertisementAck{}
		if err := proto.Unmarshal(meta.Payload, ack); err != nil {
			return err
		}
		slog.Debug("artery: received compression table ack", "manifest", manifest, "version", ack.GetVersion(), "from", ack.GetFrom())
		return nil

	default:
		if meta.SerializerId == 6 {
			// MessageContainerSerializer (ID=6) wraps ActorSelectionMessage.
			// Akka sends cluster heartbeats via actorSelection using serializer 6.
			// Decode the SelectionEnvelope and forward inner cluster messages so
			// our failure-detector keeps receiving HBR responses from Go.
			env := &gproto_remote.SelectionEnvelope{}
			if err := proto.Unmarshal(meta.Payload, env); err == nil {
				innerSID := env.GetSerializerId()
				innerManifest := string(env.GetMessageManifest())

				// Forward cluster messages (sid=5) to the cluster manager.
				if innerSID == 5 && innerManifest != "" && assoc.nodeMgr.clusterMgr != nil {
					assoc.mu.RLock()
					remote := assoc.remote
					assoc.mu.RUnlock()
					slog.Debug("artery: forwarding actorSelection cluster message", "manifest", innerManifest)
					return assoc.nodeMgr.clusterMgr.HandleIncomingClusterMessage(
						ctx, env.GetEnclosedMessage(), innerManifest, ToClusterUniqueAddress(remote))
				}

				// Handle Identify (sid=16) — respond with ActorIdentity so Pekko's
				// actorSelection(...).resolveOne() can discover Go actors.
				if innerSID == MiscMessageSerializerID {
					assoc.handleIdentify(meta, env)
					return nil
				}
			}
			return nil
		}
		slog.Debug("artery: unidentified control message", "manifest", manifest, "serializerId", meta.SerializerId)
		return nil
	}
}

// handleIdentify responds to Pekko's Identify messages inside ActorSelectionMessages.
// This enables actorSelection(...).resolveOne() from Pekko to discover Go actors.
func (assoc *GekkaAssociation) handleIdentify(meta *ArteryMetadata, env *gproto_remote.SelectionEnvelope) {
	// Decode the Identify proto from the enclosed message.
	identify := &gproto_remote.Identify{}
	if err := proto.Unmarshal(env.GetEnclosedMessage(), identify); err != nil {
		slog.Debug("artery: failed to decode Identify", "error", err)
		return
	}

	// Resolve the recipient path from the ActorSelection elements.
	recipientPath := meta.Recipient.GetPath()

	// Build the actor ref path for the reply.
	la := assoc.nodeMgr.LocalAddr
	actorRefPath := fmt.Sprintf("%s://%s@%s:%d%s",
		la.GetProtocol(), la.GetSystem(), la.GetHostname(), la.GetPort(), recipientPath)

	// Build ActorIdentity reply.  Ref is always set (the actor exists from
	// Pekko's perspective — Go's registered actors serve as remote endpoints).
	identity := &gproto_remote.ActorIdentity{
		CorrelationId: identify.GetMessageId(),
		Ref:           &gproto_remote.ProtoActorRef{Path: proto.String(actorRefPath)},
	}

	identityBytes, err := proto.Marshal(identity)
	if err != nil {
		slog.Warn("artery: failed to marshal ActorIdentity", "error", err)
		return
	}

	// Send ActorIdentity back to the sender.  The sender path is in the Artery
	// envelope's sender field — that's where Pekko's resolveOne() waits.
	senderPath := ""
	if meta.Sender != nil {
		senderPath = meta.Sender.GetPath()
	}
	if senderPath == "" {
		slog.Debug("artery: Identify has no sender, cannot reply")
		return
	}

	// Send ActorIdentity via the outbound association to Pekko.
	// In Artery TCP, connections are unidirectional.
	remoteAddr := assoc.remote.GetAddress()
	outAssoc, ok := assoc.nodeMgr.GetGekkaAssociationByHost(remoteAddr.GetHostname(), remoteAddr.GetPort())
	if !ok {
		slog.Warn("artery: no outbound association for ActorIdentity reply")
		return
	}

	// Use SendWithSender to properly frame the reply with the actor ref as sender.
	if err := outAssoc.SendWithSender(senderPath, actorRefPath, identityBytes, MiscMessageSerializerID, "B"); err != nil {
		slog.Warn("artery: failed to send ActorIdentity", "error", err)
		return
	}
	slog.Debug("artery: sent ActorIdentity", "recipient", senderPath, "actorRef", actorRefPath)
}

func (assoc *GekkaAssociation) handleHandshakeReq(req *gproto_remote.HandshakeReq) error {
	slog.Debug("artery: received HandshakeReq", "from", req.From.String(), "role", assoc.role)

	// Validate that the 'To' address matches our local node (Pekko protocol requirement).
	if toSys := req.GetTo().GetSystem(); toSys != "" {
		if localSys := assoc.nodeMgr.LocalAddr.GetSystem(); toSys != localSys {
			return fmt.Errorf("Handshake rejected: To system %q != local system %q", toSys, localSys)
		}
	}

	// Quarantine guard: reject reconnection from a permanently quarantined UID.
	if uid := req.GetFrom().GetUid(); uid != 0 && assoc.nodeMgr.IsQuarantined(uid) {
		slog.Warn("artery: rejecting HandshakeReq from quarantined UID", "uid", uid)
		// Inform the remote that it is quarantined and close.
		assoc.SendQuarantined(req.From)
		assoc.mu.Lock()
		assoc.state = QUARANTINED
		if assoc.conn != nil {
			assoc.conn.Close()
		}
		assoc.mu.Unlock()
		return fmt.Errorf("artery: HandshakeReq rejected — UID %d is quarantined", uid)
	}

	assoc.mu.Lock()
	assoc.remote = req.From
	assoc.state = ASSOCIATED
	assoc.mu.Unlock()

	assoc.nodeMgr.RegisterAssociation(req.From, assoc)

	// Symmetric Handshake: check if there's an outbound association waiting for a Handshake from this same remote node.
	if assoc.role == INBOUND {
		addr := req.From.GetAddress()
		nm := assoc.nodeMgr
		nm.mu.RLock()
		var matched *GekkaAssociation          // OUTBOUND in WAITING/INITIATED state
		var outboundToRemote *GekkaAssociation // any OUTBOUND to this remote (any state)

		normalize := func(h string) string {
			if h == "localhost" || h == "127.0.0.1" || h == "::1" {
				return "localhost"
			}
			return h
		}

		for k, a := range nm.associations {
			a.mu.RLock()
			isOutbound := a.role == OUTBOUND
			isWaiting := a.state == INITIATED || a.state == WAITING_FOR_HANDSHAKE
			var hostMatch bool
			var aHost string
			var aPort uint32
			if a.remote != nil && a.remote.Address != nil {
				aHost = a.remote.Address.GetHostname()
				aPort = a.remote.Address.GetPort()
				hostMatch = normalize(aHost) == normalize(addr.GetHostname()) &&
					aPort == addr.GetPort()
			}
			a.mu.RUnlock()

			slog.Debug("artery: handleHandshakeReq candidate", "key", k, "role", a.role, "state", a.state, "host", aHost, "port", aPort, "match", hostMatch)

			if isOutbound && hostMatch {
				if outboundToRemote == nil {
					outboundToRemote = a
				}
				if isWaiting && matched == nil {
					matched = a
					slog.Debug("artery: handleHandshakeReq matched association", "key", k)
				}
			}
		}
		nm.mu.RUnlock()

		// HandshakeRsp must be sent on Go's OUTBOUND TCP connection to Pekko.
		// In Artery-TCP, Pekko's outbound sockets are WRITE-ONLY — writing bytes
		// back on them causes "Unexpected incoming bytes" and quarantine/reset.
		// Pekko reads HandshakeRsp on its INBOUND (= Go's OUTBOUND to Pekko).
		//
		// If no OUTBOUND exists (e.g. when Go is the seed and Pekko joins for
		// the first time), we initiate one now. Without this, the fallback of
		// writing HandshakeRsp on the INBOUND socket causes Pekko to reset
		// the connection and Pekko can never join Go-seeded clusters.
		if outboundToRemote == nil {
			fromAddr := req.From.GetAddress()
			// No outbound to the remote exists yet — this happens when
			// Go is the seed and Pekko connects for the first time.
			// Initiate a reverse connection ASYNCHRONOUSLY. The first
			// HandshakeRsp will use the INBOUND fallback (which Pekko
			// may reject), but by the time Scala retries InitJoin (~5s)
			// the outbound will be ASSOCIATED and messages will flow.
			nm.mu.Lock()
			if nm.pendingDials == nil {
				nm.pendingDials = make(map[string]bool)
			}
			dialKey := fmt.Sprintf("%s:%d", fromAddr.GetHostname(), fromAddr.GetPort())
			if !nm.pendingDials[dialKey] {
				nm.pendingDials[dialKey] = true
				slog.Info("artery: no OUTBOUND to remote — initiating async control connection",
					"host", fromAddr.GetHostname(), "port", fromAddr.GetPort())
				go nm.DialRemote(context.Background(), fromAddr)
			}
			nm.mu.Unlock()
		}
		{
			localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
			rspProto := &gproto_remote.MessageWithAddress{Address: localUA}
			if rspPayload, err2 := proto.Marshal(rspProto); err2 == nil {
				if frame, err2 := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", rspPayload, true); err2 == nil {
					target := outboundToRemote
					if target != nil {
						slog.Debug("artery: sending HandshakeRsp via OUTBOUND association")
					} else {
						// Last resort fallback for unit tests without a real TCP stack.
						target = assoc
						slog.Warn("artery: no OUTBOUND association available, sending HandshakeRsp via INBOUND (may cause reset)")
					}
					select {
					case target.outbox <- frame:
					default:
						slog.Warn("artery: HandshakeRsp outbox full, dropping response")
					}
				}
			}
		}

		// Symmetric optimization: if this node also has an OUTBOUND assoc to
		// the same remote (e.g. both nodes dialled each other simultaneously),
		// complete it directly without waiting for an extra round-trip.
		if matched != nil {
			slog.Info("artery: completing matching OUTBOUND association")
			matched.mu.Lock()
			matched.remote = req.From
			matched.state = ASSOCIATED
			select {
			case <-matched.Handshake:
			default:
				close(matched.Handshake)
			}
			for _, msg := range matched.pending {
				select {
				case matched.outbox <- msg:
				default:
					slog.Warn("artery: outbox full, dropping pending frame during handshake flush")
				}
			}
			matched.pending = nil
			matched.mu.Unlock()
			nm.RegisterAssociation(req.From, matched)
		}

		return nil
	}

	// OUTBOUND: flush pending and send HandshakeRsp (if we were the one initiating)
	assoc.mu.Lock()
	for _, msg := range assoc.pending {
		select {
		case assoc.outbox <- msg:
		default:
			slog.Warn("artery: outbox full, dropping pending frame during handshake flush")
		}
	}
	assoc.pending = nil
	assoc.mu.Unlock()

	localUA := &gproto_remote.UniqueAddress{Address: assoc.nodeMgr.LocalAddr, Uid: proto.Uint64(assoc.localUid)}
	rsp := &gproto_remote.MessageWithAddress{Address: localUA}

	// Write HandshakeRsp to outbox
	payload, err := proto.Marshal(rsp)
	if err != nil {
		return err
	}
	frame, err := BuildArteryFrame(int64(assoc.localUid), actor.ArteryInternalSerializerID, "", "", "e", payload, true)
	if err != nil {
		return err
	}
	slog.Debug("artery: sending HandshakeRsp (e)", "uid", assoc.localUid)
	select {
	case assoc.outbox <- frame:
	default:
		slog.Warn("artery: outbox full, dropping HandshakeRsp frame")
	}
	return nil
}

func (assoc *GekkaAssociation) handleHandshakeRsp(mwa *gproto_remote.MessageWithAddress) error {
	slog.Debug("artery: received HandshakeRsp", "from", mwa.Address.String())

	assoc.nodeMgr.mu.RLock()
	// Create a list of candidates to avoid holding nodeMgr lock while locking individuals
	candidates := make([]*GekkaAssociation, 0, len(assoc.nodeMgr.associations))
	for _, a := range assoc.nodeMgr.associations {
		candidates = append(candidates, a)
	}
	assoc.nodeMgr.mu.RUnlock()

	var matched *GekkaAssociation
	for _, a := range candidates {
		a.mu.RLock()
		isOutbound := a.role == OUTBOUND
		isWaiting := a.state == INITIATED || a.state == WAITING_FOR_HANDSHAKE || a.state == ASSOCIATED

		var match bool
		if a.remote != nil && mwa.Address != nil && mwa.Address.Address != nil {
			aHost, aPort := a.remote.Address.GetHostname(), a.remote.Address.GetPort()
			mHost, mPort := mwa.Address.Address.GetHostname(), mwa.Address.Address.GetPort()

			normalize := func(h string) string {
				if h == "localhost" || h == "127.0.0.1" {
					return "127.0.0.1"
				}
				return h
			}
			match = normalize(aHost) == normalize(mHost) && aPort == mPort
			slog.Debug("artery: handleHandshakeRsp candidate", "match", match, "host", aHost, "port", aPort)
		} else {
			slog.Debug("artery: handleHandshakeRsp candidate", "match", false, "hasRemote", a.remote != nil, "hasMwaAddr", mwa.Address != nil)
		}
		a.mu.RUnlock()

		if isOutbound && isWaiting && match {
			slog.Info("artery: found matching outbound association", "state", a.state)
			matched = a
			break
		}
	}

	if matched != nil {
		matched.mu.Lock()
		matched.remote = mwa.Address
		matched.state = ASSOCIATED

		// Unblock anybody waiting on this
		select {
		case <-matched.Handshake:
			// already closed
		default:
			close(matched.Handshake)
		}

		// Flush pending messages
		for _, msg := range matched.pending {
			select {
			case matched.outbox <- msg:
			default:
				slog.Warn("artery: outbox full, dropping pending frame during handshakeRsp flush")
			}
		}
		matched.pending = nil
		matched.mu.Unlock()

		// Re-register with full UniqueAddress (including UID from Pekko)
		assoc.nodeMgr.RegisterAssociation(mwa.Address, matched)
	} else {
		slog.Warn("artery: no matching outbound association found for HandshakeRsp")
	}

	return nil
}

// ---------------------------------------------------------------------------
// UDP transport extensions
// ---------------------------------------------------------------------------

// Dispatch is an exported wrapper around the unexported dispatch method.
// It is called by the UdpArteryHandler to route inbound Aeron DATA frames
// through the same Artery message pipeline used by the TCP transport.
func (assoc *GekkaAssociation) Dispatch(ctx context.Context, meta *ArteryMetadata) error {
	return assoc.dispatch(ctx, meta)
}

// GetOrCreateUDPAssociation returns an existing association for src, or creates
// a new INBOUND UDP association wired to udpH for outbound responses.
//
// The new association has no TCP conn; its outbox drainer forwards frames via
// udpH.SendFrame(src, AeronStreamControl, frame).
// getAnyAssociationByHost returns the first association (INBOUND or OUTBOUND)
// for the given host:port pair.  Used by the UDP inbound path to find previously
// created inbound associations without filtering to OUTBOUND-only.
func (nm *NodeManager) getAnyAssociationByHost(host string, port uint32) (*GekkaAssociation, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for _, assoc := range nm.associations {
		assoc.mu.RLock()
		remote := assoc.remote
		assoc.mu.RUnlock()
		if remote != nil && remote.Address.GetHostname() == host && remote.Address.GetPort() == port {
			return assoc, true
		}
	}
	return nil, false
}

func (nm *NodeManager) GetOrCreateUDPAssociation(src *net.UDPAddr, udpH *UdpArteryHandler) *GekkaAssociation {
	// Fast path: look up by physical UDP source address.  This map is keyed by
	// the ephemeral media-driver port (e.g. "127.0.0.1:62159") and is never
	// overwritten when handleHandshakeReq updates assoc.remote to the canonical
	// Akka address, so it remains valid for the lifetime of the session.
	srcKey := src.String()
	nm.mu.RLock()
	if assoc, ok := nm.udpSrcAssoc[srcKey]; ok {
		nm.mu.RUnlock()
		return assoc
	}
	nm.mu.RUnlock()

	// Also check whether the src is the canonical Akka address (e.g. a direct
	// OUTBOUND association to 127.0.0.1:2561).
	if assoc, ok := nm.getAnyAssociationByHost(src.IP.String(), uint32(src.Port)); ok {
		return assoc
	}

	assoc := &GekkaAssociation{
		state:      INITIATED,
		role:       INBOUND,
		nodeMgr:    nm,
		lastSeen:   time.Now(),
		Handshake:  make(chan struct{}),
		localUid:   nm.localUid,
		outbox:     make(chan []byte, 512),
		streamId:   AeronStreamControl,
		udpHandler: udpH,
		udpDst:     src,
		remote: &gproto_remote.UniqueAddress{
			Address: &gproto_remote.Address{
				Protocol: nm.LocalAddr.Protocol,
				System:   nm.LocalAddr.System,
				Hostname: proto.String(src.IP.String()),
				Port:     proto.Uint32(uint32(src.Port)),
			},
			Uid: proto.Uint64(0),
		},
	}
	nm.RegisterAssociation(assoc.remote, assoc)

	// Pin the ephemeral UDP source address to this association permanently.
	// handleHandshakeReq will later overwrite assoc.remote with the canonical
	// Akka address, but udpSrcAssoc keeps the original src → assoc binding.
	nm.mu.Lock()
	nm.udpSrcAssoc[srcKey] = assoc
	nm.mu.Unlock()

	// Start outbox drainer: sends frames via Aeron UDP back to src.
	go func() {
		for frame := range assoc.outbox {
			if err := udpH.SendFrame(src, AeronStreamControl, frame); err != nil {
				slog.Warn("aeron-udp: inbound assoc outbox error", "src", src, "error", err)
			}
		}
	}()

	slog.Info("aeron-udp: new inbound UDP association created", "src", src)
	return assoc
}

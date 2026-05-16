/*
 * outbound_streamid2_handshakereq_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"net"
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestHandshakeReqOnOutboundStreamId2_RoutesViaControlNotSiblingCap1Outbox
// pins the bug 1 (outbox saturation) regression. Every multi-jvm run on
// current main emits hundreds of "outbox full, dropping HandshakeRsp frame"
// warnings at ~1 Hz from association.go:3946 (the OUTBOUND branch of
// handleHandshakeReq). Root cause: when Pekko writes a HandshakeReq on one
// of gekka's OUTBOUND streamId=2 TCPs (e.g. during stream re-handshake or
// the symmetric handshake convergence window), the dispatch lands on the
// streamId=2 OUTBOUND sibling assoc — whose outbox channel is deliberately
// allocated with capacity 1 (association.go:2530 — intended unused, all
// real writes pivot through per-lane outboxes via outboxFor). The OUTBOUND
// branch of handleHandshakeReq writes the response to `assoc.outbox`
// directly, bypassing outboxFor. First write fills the cap=1 buffer; every
// subsequent retry drops, triggering the warning at the peer's
// handshake-retry-interval (~1 s).
//
// Acceptance: an OUTBOUND streamId=2 sibling assoc receiving a HandshakeReq
// must route the HandshakeRsp via the linked OUTBOUND streamId=1 control
// sibling's outbox (capacity 20000 = EffectiveOutboundControlQueueSize),
// not via its own cap=1 placeholder outbox.
func TestHandshakeReqOnOutboundStreamId2_RoutesViaControlNotSiblingCap1Outbox(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 1)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	const remoteUid = uint64(0xC0FFEE)
	remote := &gproto_remote.UniqueAddress{Address: remoteAddr, Uid: proto.Uint64(remoteUid)}

	// OUTBOUND streamId=1 control sibling — the correct destination for
	// any control-class frame. Cap is the realistic control-queue size.
	controlServer, controlClient := net.Pipe()
	t.Cleanup(func() { _ = controlServer.Close(); _ = controlClient.Close() })
	control := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      controlClient,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, nm.EffectiveOutboundControlQueueSize()),
		streamId:  1,
	}
	control.remote.Store(remote)
	close(control.Handshake)
	nm.RegisterAssociation(remote, control)

	// OUTBOUND streamId=2 sibling — modelled exactly as dialOrdinarySibling
	// constructs it: outbox capacity 1 (deliberately placeholder), with
	// the bi-directional ordinarySibling pointer linking back to control.
	sib := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 1), // ← the cap=1 placeholder
		streamId:  2,
	}
	sib.remote.Store(remote)
	close(sib.Handshake)
	control.mu.Lock()
	control.ordinarySibling = sib
	control.mu.Unlock()
	sib.mu.Lock()
	sib.ordinarySibling = control
	sib.mu.Unlock()

	// Drive a HandshakeReq into the OUTBOUND streamId=2 sibling — exactly
	// what runLaneReadLoop dispatches when Pekko writes a HandshakeReq on
	// one of gekka's streamId=2 outbound TCPs (live-diag triggers this
	// repeatedly at the peer's handshake-retry interval).
	req := &gproto_remote.HandshakeReq{From: remote, To: nm.LocalAddr}

	// Pre-fill the cap=1 sibling outbox so the OUTBOUND branch's
	// `case assoc.outbox <- frame:` non-blocking write must fall through
	// to default. This mirrors steady-state production where any prior
	// stray write (or even the rsp from the previous iteration) already
	// occupies the slot.
	sib.outbox <- []byte("placeholder")

	// Invoke the actual handler — this is the code path the live diag fires.
	if err := sib.handleHandshakeReq(req); err != nil {
		t.Fatalf("handleHandshakeReq: %v", err)
	}

	// The sibling's cap=1 outbox must still hold only the original
	// placeholder — the rsp must NOT have been queued there.
	if got := len(sib.outbox); got != 1 {
		t.Errorf("sibling outbox got %d frames, want 1 (only the placeholder) — rsp leaked onto cap=1 placeholder", got)
	}

	// The HandshakeRsp must instead be in the OUTBOUND control sibling's
	// outbox, where it can actually reach the peer and clear the retry
	// storm.
	if got := len(control.outbox); got != 1 {
		t.Errorf("control sibling outbox got %d frames, want 1 — rsp not routed via control (this is what drives the 1 Hz outbox-full storm)", got)
	}
}

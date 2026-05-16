/*
 * inbound_quarantine_rsp_routing_test.go
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

// TestSendQuarantined_RoutesToOutboundNotInboundSocket pins the same class of
// regression that f69bed6 closed for ArteryHeartbeatRsp, but for the
// Quarantined notification path. SendQuarantined wrote the frame to
// assoc.outbox unconditionally; when called on an INBOUND assoc the lane
// writer goroutine started by ProcessConnection drains assoc.outbox to
// assoc.conn — the socket the peer dialed gekka with (peer's outbound,
// write-only-from-peer's-perspective) — and Pekko's "Unexpected incoming
// bytes in outbound connection" path quarantines the whole association.
//
// Real production trigger: handleHandshakeReq at association.go:3573 calls
// assoc.SendQuarantined(req.From) on the INBOUND assoc that received the
// HandshakeReq, every time a quarantined UID retries the handshake.
func TestSendQuarantined_RoutesToOutboundNotInboundSocket(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 1)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	const remoteUid = uint64(0xDEADBEEF)

	// OUTBOUND streamId=1 control assoc to the peer — the correct
	// destination for any control frame addressed at the peer.
	outboundServer, outboundClient := net.Pipe()
	t.Cleanup(func() { _ = outboundServer.Close(); _ = outboundClient.Close() })
	outboundRemote := &gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(remoteUid),
	}
	outbound := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      outboundClient,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		streamId:  1,
	}
	outbound.remote.Store(outboundRemote)
	close(outbound.Handshake)
	nm.RegisterAssociation(outboundRemote, outbound)

	// INBOUND streamId=1 assoc representing Pekko's outbound control
	// socket landing on gekka. This is the assoc handleHandshakeReq would
	// be holding when it rejects a HandshakeReq from a quarantined UID and
	// calls assoc.SendQuarantined(req.From).
	inboundServer, inboundClient := net.Pipe()
	t.Cleanup(func() { _ = inboundServer.Close(); _ = inboundClient.Close() })
	inbound := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      INBOUND,
		conn:      inboundClient,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		streamId:  1,
	}
	inbound.remote.Store(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(remoteUid),
	})
	close(inbound.Handshake)

	// Drive SendQuarantined on the INBOUND assoc — the production trigger.
	inbound.SendQuarantined(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(remoteUid),
	})

	// CRITICAL: nothing must have been queued onto the INBOUND outbox.
	// The writer goroutine that drains assoc.outbox writes to assoc.conn,
	// which for an INBOUND assoc is the socket Pekko dialed gekka with
	// (Pekko's outbound socket). Writing any byte back triggers Pekko's
	// "Unexpected incoming bytes" + quarantine cascade.
	if got := len(inbound.outbox); got != 0 {
		t.Errorf("INBOUND outbox got %d frames, want 0 — Quarantined frame leaked onto INBOUND socket (Pekko will quarantine us)", got)
	}

	// The notification must instead have been routed onto the OUTBOUND
	// control assoc's outbox (gekka's own outgoing socket).
	if got := len(outbound.outbox); got != 1 {
		t.Errorf("OUTBOUND outbox got %d frames, want 1 — Quarantined frame was not routed via gekka's OUTBOUND socket to peer", got)
	}
}

// TestSendQuarantined_OutboundDirectStillWorks verifies the existing
// behaviour for the normal case: when SendQuarantined is called on an
// OUTBOUND assoc itself (e.g. the UID-mismatch path in RegisterAssociation
// when `existing` is OUTBOUND), the frame goes on that assoc's outbox.
func TestSendQuarantined_OutboundDirectStillWorks(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 1)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}

	outboundServer, outboundClient := net.Pipe()
	t.Cleanup(func() { _ = outboundServer.Close(); _ = outboundClient.Close() })
	outbound := &GekkaAssociation{
		state:     ASSOCIATED,
		role:      OUTBOUND,
		conn:      outboundClient,
		nodeMgr:   nm,
		Handshake: make(chan struct{}),
		localUid:  nm.localUid,
		outbox:    make(chan []byte, 16),
		streamId:  1,
	}
	outbound.remote.Store(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(0xDEADBEEF),
	})
	close(outbound.Handshake)

	outbound.SendQuarantined(&gproto_remote.UniqueAddress{
		Address: remoteAddr,
		Uid:     proto.Uint64(0xDEADBEEF),
	})

	if got := len(outbound.outbox); got != 1 {
		t.Errorf("OUTBOUND outbox got %d frames, want 1", got)
	}
}

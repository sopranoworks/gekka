/*
 * inbound_heartbeat_rsp_routing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"net"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestArteryHeartbeatRsp_RoutesToOutboundNotInboundSocket pins the regression
// for the live-diag quarantine cascade. When Pekko's RemoteWatcher sends an
// ArteryHeartbeat, it travels over Pekko's outbound TCP socket which gekka
// sees as an INBOUND connection. Pekko's outbound is write-only from Pekko's
// perspective — any bytes flowing back on that socket trigger Pekko's
// "Unexpected incoming bytes in outbound connection" path and Pekko
// quarantines gekka.
//
// Same class of bug as MEMORY.md fix #15 (HandshakeRsp on INBOUND socket):
// the response must travel via gekka's OWN OUTBOUND socket to the peer.
//
// This test fails on current main because case "m" in handleControlMessage
// writes the ArteryHeartbeatRsp frame unconditionally to `assoc.outbox`
// where `assoc` is the INBOUND assoc the heartbeat arrived on.
func TestArteryHeartbeatRsp_RoutesToOutboundNotInboundSocket(t *testing.T) {
	nm, _ := newOutboundLanesNodeManager(t, 1)

	remoteAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Peer"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	const remoteUid = uint64(0xCAFEBABE)

	// Pre-register an OUTBOUND streamId=1 control association to the peer
	// (gekka's own outgoing socket — the only correct destination for
	// any response frame to the peer).
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

	// Build the INBOUND streamId=1 assoc the heartbeat arrives on.
	// This represents Pekko's outbound control socket landing on gekka.
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

	// Dispatch an ArteryHeartbeat frame on the INBOUND assoc. Empty payload
	// matches Pekko's ArteryHeartbeat singleton encoding.
	meta := &ArteryMetadata{
		SerializerId:    actor.ArteryInternalSerializerID,
		MessageManifest: []byte("m"),
		Payload:         nil,
	}
	if err := inbound.dispatch(context.Background(), meta); err != nil {
		t.Fatalf("dispatch ArteryHeartbeat: %v", err)
	}

	// CRITICAL: nothing must have been queued onto the INBOUND outbox.
	// The writer goroutine that drains assoc.outbox writes to assoc.conn,
	// which for an INBOUND assoc is the socket Pekko dialed gekka with
	// (Pekko's outbound socket). Writing any byte back triggers Pekko's
	// "Unexpected incoming bytes" + quarantine cascade.
	if got := len(inbound.outbox); got != 0 {
		t.Errorf("INBOUND outbox got %d frames, want 0 — ArteryHeartbeatRsp leaked onto INBOUND socket (Pekko will quarantine us)", got)
	}

	// The response must instead have been routed onto the OUTBOUND
	// control assoc's outbox (gekka's own outgoing socket — the correct
	// direction).
	if got := len(outbound.outbox); got != 1 {
		t.Errorf("OUTBOUND outbox got %d frames, want 1 — ArteryHeartbeatRsp was not routed to peer's INBOUND (= gekka's OUTBOUND)", got)
	}
}

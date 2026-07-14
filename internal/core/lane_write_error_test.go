/*
 * lane_write_error_test.go
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
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestLaneWriteError_UnregistersDeadOutbound pins that an OUTBOUND
// association whose lane writer dies on a write error stops being
// selectable for new sends. The lane writer exits on the first write
// failure (e.g. the peer closed this socket as a handshake-race
// duplicate); if the association stays registered as ASSOCIATED,
// GetGekkaAssociationByHost keeps preferring it for every subsequent
// send and the frames are enqueued into an outbox nobody drains — a
// silent, permanent, DIRECTED blackout toward that peer. Live symptom
// (showcase updfd-v1 / updfd-probe1): every heartbeat and heartbeat
// reply g1 sent toward g3 vanished while g3→g1 flowed normally; g1's
// seeded FD record for g3 starved, both flagged each other Unreachable
// ~22s after monitoring started, and the JVM SBR downed a perfectly
// healthy cluster.
func TestLaneWriteError_UnregistersDeadOutbound(t *testing.T) {
	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.9"),
			Port:     proto.Uint32(2553),
		},
		Uid: proto.Uint64(7),
	}

	client, server := net.Pipe()
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		conn:     client,
		nodeMgr:  nm,
		streamId: 1,
		outbox:   make(chan []byte, 16),
	}
	assoc.remote.Store(remoteUA)
	nm.RegisterAssociation(remoteUA, assoc)

	if _, ok := nm.GetGekkaAssociationByHost("10.0.0.9", 2553); !ok {
		t.Fatal("sanity: association must be selectable before the write error")
	}

	lane := &outboundLane{idx: 0, conn: client, outbox: assoc.outbox}
	go assoc.startLaneWriter(context.Background(), lane)

	// Peer closes this socket (handshake-race duplicate cleanup) — the
	// next write errors and the lane writer exits.
	_ = server.Close()
	assoc.outbox <- []byte("frame toward a closed peer")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := nm.GetGekkaAssociationByHost("10.0.0.9", 2553); !ok {
			return // dead outbound no longer selectable — next send re-dials
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("write-dead OUTBOUND association is still returned by GetGekkaAssociationByHost — every subsequent send to the peer is enqueued into an outbox nobody drains (silent directed blackout: heartbeats/replies vanish, the peer's FD record starves, SBR downs a healthy member)")
}

// TestByHostLookup_NeverFallsBackToOrdinarySibling pins that
// GetGekkaAssociationByHost's fallback never returns a streamId=2
// ordinary-stream sibling: its base outbox is a deliberate cap-1 stub
// (all its traffic flows through per-lane outboxes selected by
// outboxFor's lane pivot), while cluster/control messages sent through
// assoc.Send always target the base outbox — so selecting the sibling
// for a direct send wedges after ONE frame ("association outbox full",
// remote …, stream 2, cap 1: the updfd-v1 WARN storm, one failed
// heartbeat reply per second, failing the strict Gate-2 window).  When
// the control association is gone (writer-dead corpse skipped), the
// lookup must return not-found so the Router re-dials a fresh control
// stream.
func TestByHostLookup_NeverFallsBackToOrdinarySibling(t *testing.T) {
	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.7"),
			Port:     proto.Uint32(2555),
		},
		Uid: proto.Uint64(11),
	}

	// Healthy streamId=2 sibling with a live lane writer — the only
	// registered association for the peer (the control corpse has been
	// skipped/unregistered).
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	sibling := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		streamId: 2,
		outbox:   make(chan []byte, 1), // the deliberate stub
		lanes:    []*outboundLane{{idx: 0, conn: client, outbox: make(chan []byte, 16)}},
	}
	sibling.remote.Store(remoteUA)
	nm.RegisterAssociation(remoteUA, sibling)
	go sibling.startLaneWriter(context.Background(), sibling.lanes[0])

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sibling.writersLive.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got, ok := nm.GetGekkaAssociationByHost("10.0.0.7", 2555); ok {
		t.Fatalf("by-host lookup returned the streamId=%d ordinary sibling — direct sends to it target its cap-1 stub outbox and wedge after one frame; with no control association the lookup must return not-found so the Router re-dials", got.streamId)
	}
}

// TestWriterlessOutbound_NotSelectable pins the invariant the showcase
// blackout violated through a DIFFERENT writer-exit path: an OUTBOUND
// association whose lane writer has exited — for ANY reason, not only a
// write error (live case: the handshake-race handover closed the early
// dial's writer 400µs after the incoming HandshakeReq matched the
// association and marked it ASSOCIATED) — must stop being selectable for
// new sends. In the updfd probe every one of g1's 140 heartbeat frames
// toward g3 was routed to the writerless object because it remained the
// only OUTBOUND-role, streamId-1, ASSOCIATED candidate in the registry.
func TestWriterlessOutbound_NotSelectable(t *testing.T) {
	localAddr := &gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("localSystem"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2552),
	}
	nm := NewNodeManager(localAddr, 0)

	remoteUA := &gproto_remote.UniqueAddress{
		Address: &gproto_remote.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("remoteSystem"),
			Hostname: proto.String("10.0.0.8"),
			Port:     proto.Uint32(2554),
		},
		Uid: proto.Uint64(9),
	}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		conn:     client,
		nodeMgr:  nm,
		streamId: 1,
		outbox:   make(chan []byte, 16),
	}
	assoc.remote.Store(remoteUA)
	nm.RegisterAssociation(remoteUA, assoc)

	lane := &outboundLane{idx: 0, conn: client, outbox: assoc.outbox}
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		assoc.startLaneWriter(writerCtx, lane)
		close(done)
	}()

	// Writer alive: the association must be selectable.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := nm.GetGekkaAssociationByHost("10.0.0.8", 2554); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sanity: association with a live writer must be selectable")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Writer exits WITHOUT a write error (handover/teardown path).
	cancelWriter()
	<-done

	if _, ok := nm.GetGekkaAssociationByHost("10.0.0.8", 2554); ok {
		t.Fatal("OUTBOUND association with NO live lane writer is still returned by GetGekkaAssociationByHost — frames sent to it are enqueued into an outbox nobody drains (the g1→g3 showcase blackout: handshake-race handover killed the early dial's writer, the object stayed the only eligible OUTBOUND, and every heartbeat/reply toward the peer vanished)")
	}
}

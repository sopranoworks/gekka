/*
 * idle_sweep_send_activity_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"net"
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// These tests pin the "used" semantics of the idle-outbound sweeps to
// Pekko's: an outbound stream is idle when it has not been USED FOR
// SENDING for stop-idle-outbound-after (reference.conf: "Outbound
// streams are stopped when they haven't been used for this duration.
// They are started again when new messages are sent.").
//
// gekka's associations track inbound activity in lastSeen — but Artery
// TCP outbound connections are write-only, so lastSeen never advances on
// them no matter how much traffic gekka sends. Judging idleness by
// lastSeen alone turned SweepIdleOutboundStop into a 5-minute timebomb:
// every outbound association (including control streams carrying
// heartbeats every second) was closed exactly stop-idle-outbound-after
// after creation. In the 8-node showcase this killed g1's control
// streams at join+300s, its cluster heartbeats stopped, the peers'
// failure detectors flagged it, and the JVM-side SBR downed the entire
// cluster (observed 2026-07-13, run showcase-obs1b-20260713).

func sendActivityTestAssoc(nm *NodeManager, uid uint64, host string) (*GekkaAssociation, *gproto_remote.UniqueAddress) {
	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr(host, 2551, "Remote"),
		Uid:     proto.Uint64(uid),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 8),
		streamId: 1,
		lastSeen: time.Now().Add(-time.Hour), // inbound side silent: write-only conn
	}
	assoc.remote.Store(remote)
	nm.RegisterAssociation(remote, assoc)
	return assoc, remote
}

// TestSweepIdleOutboundStop_SendActivityPreventsStop: an association that
// is actively sending must never be judged idle, even though its inbound
// lastSeen is stale (outbound Artery TCP conns never receive frames).
func TestSweepIdleOutboundStop_SendActivityPreventsStop(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 50 * time.Millisecond

	assoc, remote := sendActivityTestAssoc(nm, 1777, "10.0.1.1")

	if err := assoc.Send("pekko://Remote@10.0.1.1:2551/user/x", []byte("hb"), 17, "m"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if n := nm.SweepIdleOutboundStop(); n != 0 {
		t.Errorf("SweepIdleOutboundStop stopped %d, want 0 — active sender judged idle", n)
	}
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Fatal("actively-sending association must survive the idle-stop sweep")
	}
}

// TestSweepIdleOutboundStop_TrulyIdleAssociationStopped guards the
// legitimate Pekko behaviour: no sends AND no inbound activity for the
// threshold → the association is stopped and deregistered.
func TestSweepIdleOutboundStop_TrulyIdleAssociationStopped(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 50 * time.Millisecond

	assoc, remote := sendActivityTestAssoc(nm, 1778, "10.0.1.2")
	assoc.backdateSendActivityForTest(time.Now().Add(-time.Hour))

	// Lane conns (multi-lane siblings) must be closed by the stop, not
	// leaked past deregistration.
	client, server := net.Pipe()
	defer server.Close()
	assoc.mu.Lock()
	assoc.lanes = []*outboundLane{{idx: 0, conn: client, outbox: make(chan []byte, 1)}}
	assoc.mu.Unlock()

	if n := nm.SweepIdleOutboundStop(); n != 1 {
		t.Errorf("SweepIdleOutboundStop stopped %d, want 1", n)
	}
	if _, ok := nm.GetAssociation(remote, 1); ok {
		t.Fatal("truly idle association must be deregistered by the idle-stop sweep")
	}
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Error("lane conn was not closed by the idle-stop sweep")
	}
}

// TestSweepIdleOutboundQuarantine_SendActivityPreventsQuarantine: the
// 6-hour quarantine sweep uses the same "used" semantics.
func TestSweepIdleOutboundQuarantine_SendActivityPreventsQuarantine(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.QuarantineIdleOutboundAfter = 50 * time.Millisecond

	assoc, _ := sendActivityTestAssoc(nm, 1779, "10.0.1.3")

	if err := assoc.SendWithSender("pekko://Remote@10.0.1.3:2551/user/x", "pekko://S@127.0.0.1:2552/user/y", []byte("hi"), 17, "m"); err != nil {
		t.Fatalf("SendWithSender: %v", err)
	}

	if n := nm.SweepIdleOutboundQuarantine(); n != 0 {
		t.Errorf("SweepIdleOutboundQuarantine quarantined %d, want 0 — active sender judged idle", n)
	}
	if assoc.GetState() == QUARANTINED {
		t.Fatal("actively-sending association must not be quarantined by the idle sweep")
	}
}

// TestSendArteryHeartbeat_IsSendActivity: the per-association transport
// heartbeat (1 Hz on every outbound control assoc) counts as use — control
// streams must never idle-stop while heartbeating.
func TestSendArteryHeartbeat_IsSendActivity(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 50 * time.Millisecond

	assoc, remote := sendActivityTestAssoc(nm, 1780, "10.0.1.4")

	if err := SendArteryHeartbeat(assoc); err != nil {
		t.Fatalf("SendArteryHeartbeat: %v", err)
	}

	if n := nm.SweepIdleOutboundStop(); n != 0 {
		t.Errorf("SweepIdleOutboundStop stopped %d, want 0 — heartbeating assoc judged idle", n)
	}
	if _, ok := nm.GetAssociation(remote, 1); !ok {
		t.Fatal("heartbeating association must survive the idle-stop sweep")
	}
}

// TestSweepIdleOutboundStop_SiblingLaneSendPreventsStop: user traffic is
// enqueued through the control assoc's Send but lands on the streamId=2
// sibling's lanes — the SIBLING's registry entry is the one the sweep
// judges, so lane routing must stamp the sibling's send activity.
func TestSweepIdleOutboundStop_SiblingLaneSendPreventsStop(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)
	nm.StopIdleOutboundAfter = 50 * time.Millisecond

	control, _ := sendActivityTestAssoc(nm, 1781, "10.0.1.5")

	sibRemote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.1.5", 2551, "Remote"),
		Uid:     proto.Uint64(1781),
	}
	sibling := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		streamId: 2,
		lastSeen: time.Now().Add(-time.Hour),
		lanes: []*outboundLane{{
			idx:    0,
			outbox: make(chan []byte, 8),
		}},
	}
	sibling.remote.Store(sibRemote)
	nm.RegisterAssociation(sibRemote, sibling)
	sibling.backdateSendActivityForTest(time.Now().Add(-time.Hour))
	control.mu.Lock()
	control.ordinarySibling = sibling
	control.mu.Unlock()

	// User-serializer send through the control assoc routes onto the
	// sibling's lane 0.
	if err := control.Send("pekko://Remote@10.0.1.5:2551/user/echo", []byte("hi"), 33, "com.example.M"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(sibling.lanes[0].outbox) != 1 {
		t.Fatalf("expected the frame on the sibling lane, got %d", len(sibling.lanes[0].outbox))
	}

	stopped := nm.SweepIdleOutboundStop()
	if _, ok := nm.GetAssociation(sibRemote, 2); !ok {
		t.Fatalf("sibling carrying user traffic must survive the idle-stop sweep (stopped=%d)", stopped)
	}
}

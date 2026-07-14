/*
 * fd_feeding_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * Pins Pekko's cluster failure-detector feeding and lifecycle semantics:
 * the FD registry is fed EXCLUSIVELY from ClusterHeartbeatSender's
 * heartbeatRsp for currently-monitored ring receivers
 * (ClusterHeartbeat.scala:316-322), and per-node records are REMOVED when
 * a node leaves the monitored set (removeMember :287-290, ring-adjustment
 * cleanup :305-309). A node this node does not monitor must have NO
 * record, so reapUnreachableMembers treats it as vacuously available. FD
 * records created from unsolicited inbound traffic (received heartbeats,
 * gossip, gossip status) are not sustained by our own monitoring: when
 * that traffic legitimately stops (heartbeat-ring reshuffle on a
 * membership change, random gossip peer selection), the record starves,
 * phi crosses the threshold, and a HEALTHY node is flagged Unreachable —
 * the divfix-v1 g3→g2 false-flag mechanism.
 */

package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// fdKeyFor returns the failure-detector key the cluster manager uses for a
// UniqueAddress. Test helper.
func fdKeyFor(ua *gproto_cluster.UniqueAddress) string {
	a := ua.GetAddress()
	uid64 := uint64(ua.GetUid()) | (uint64(ua.GetUid2()) << 32)
	return fmt.Sprintf("%s:%d-%d", a.GetHostname(), a.GetPort(), uid64)
}

// hasFDRecord reports whether a per-node detector exists for ua (every
// constructed detector carries a non-zero first-heartbeat estimate).
func hasFDRecord(cm *ClusterManager, ua *gproto_cluster.UniqueAddress) bool {
	return cm.Fd.FirstHeartbeatEstimateFor(fdKeyFor(ua)) != 0
}

// TestHandleHeartbeat_DoesNotFeedClusterFD pins that RECEIVING a heartbeat
// does not create or feed an FD record for its sender. Pekko's
// ClusterHeartbeatReceiver only replies; the FD is fed by the sender side
// from the response. Feeding on receipt creates a record for a node this
// node does not monitor — a record whose starvation later false-flags a
// healthy peer.
func TestHandleHeartbeat_DoesNotFeedClusterFD(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	sender := makeUAWithDC("127.0.0.1", 2551, 51)
	hb := &gproto_cluster.Heartbeat{From: sender.GetAddress()}
	payload, err := proto.Marshal(hb)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := cm.handleHeartbeat(payload, "HB", sender); err != nil {
		t.Fatalf("handleHeartbeat: %v", err)
	}

	if hasFDRecord(cm, sender) {
		t.Fatal("receiving a Heartbeat created a cluster-FD record for its SENDER — Pekko feeds the cluster FD only from heartbeatRsp of nodes this node actively monitors (ClusterHeartbeat.scala:316-318); a traffic-fed record starves and false-flags the peer when it legitimately stops sending")
	}
}

// TestHandleGossipStatus_DoesNotFeedClusterFD pins the same rule for the
// gossip-status ingress path.
func TestHandleGossipStatus_DoesNotFeedClusterFD(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	sender := makeUAWithDC("127.0.0.1", 2551, 51)
	status := &gproto_cluster.GossipStatus{From: sender, Version: &gproto_cluster.VectorClock{}}
	payload, err := proto.Marshal(status)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := cm.handleGossipStatus(payload, "GS"); err != nil {
		t.Fatalf("handleGossipStatus: %v", err)
	}

	if hasFDRecord(cm, sender) {
		t.Fatal("receiving a GossipStatus created a cluster-FD record for its sender — Pekko never feeds the cluster FD from gossip traffic")
	}
}

// TestProcessIncomingGossip_DoesNotFeedClusterFD pins the same rule for
// the gossip-envelope ingress path.
func TestProcessIncomingGossip_DoesNotFeedClusterFD(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	r := makeUAWithDC("127.0.0.1", 2551, 51)
	rName := pekkoVclockNodeHash(r)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	addMemberWithStatus(cm, r, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, map[string]int64{cm.localHashString(): 1})
	cm.Mu.Unlock()

	remote := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{r, local},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(2)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{Seen: []int32{0}},
	}
	setClock(remote, map[string]int64{cm.localHashString(): 1, rName: 2})

	if err := cm.processIncomingGossip(remote, r); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	if hasFDRecord(cm, r) {
		t.Fatal("receiving a GossipEnvelope created a cluster-FD record for its sender — Pekko never feeds the cluster FD from gossip traffic")
	}
}

// TestHandleHeartbeatRsp_FeedsClusterFD pins that heartbeat RESPONSES do
// feed the FD — that is Pekko's one legitimate feed (the response to this
// node's own monitoring).
func TestHandleHeartbeatRsp_FeedsClusterFD(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	target := makeUAWithDC("127.0.0.1", 2551, 51)
	rsp := &gproto_cluster.HeartBeatResponse{From: target}
	payload, err := proto.Marshal(rsp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := cm.handleHeartbeatRsp(payload, "HBR"); err != nil {
		t.Fatalf("handleHeartbeatRsp: %v", err)
	}

	if !hasFDRecord(cm, target) {
		t.Fatal("HeartbeatRsp did not feed the cluster FD — responses to this node's own monitoring are Pekko's one legitimate FD feed")
	}
}

// TestApplyDetectorConfig_AcceptablePauseExtendsPhiTolerance pins that the
// configured acceptable-heartbeat-pause participates in the phi computation
// exactly as in Pekko: PhiAccrualFailureDetector.phi compares the silence
// against mean + acceptableHeartbeatPause (PhiAccrualFailureDetector.scala:202),
// so a pause of 8s keeps a briefly-quiet node available. gekka's detector
// omitted the pause term entirely (the HOCON value was parsed and dropped),
// making it ~5x more trigger-happy than a default Pekko node — a few seconds
// of heartbeat-response delay during a join storm produced a gekka-authored
// Unreachable record that fed straight into the JVM SBR (updfd-v1 collapse:
// g3 -> g1, DownAll two seconds after Gate 1).
func TestApplyDetectorConfig_AcceptablePauseExtendsPhiTolerance(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	ApplyDetectorConfig(cm, FailureDetectorConfig{
		Threshold:                8.0,
		MinStdDeviation:          100 * time.Millisecond,
		AcceptableHeartbeatPause: 8 * time.Second,
	})

	key := "127.0.0.1:2551-51"
	// One heartbeat with a 50ms first-estimate: history mean = 50ms, σ = 100ms.
	cm.Fd.HeartbeatWithEstimate(key, 50*time.Millisecond)
	time.Sleep(1200 * time.Millisecond)

	// 1.2s of silence: without the pause term (1200-50)/100 ≈ 11.5σ → φ ≫ 8
	// → false unavailable. Pekko compares against mean+8000ms → φ ≈ 0.
	if !cm.Fd.IsAvailable(key) {
		t.Fatalf("node flagged unavailable after 1.2s of silence despite acceptable-heartbeat-pause=8s (φ=%.2f) — Pekko computes φ against mean + acceptableHeartbeatPause (PhiAccrualFailureDetector.scala:202)", cm.Fd.Phi(key))
	}
}

// TestHeartbeatTask_SeedsFDRecordWhenTargetNeverResponds pins Pekko's
// trigger-first-heartbeat rule (ClusterHeartbeat.scala:219-228, 250-259):
// when a monitored target has not responded expected-response-after into
// the monitoring, the FD record is seeded ANYWAY so the φ clock starts —
// otherwise a member that dies before ever responding (or is dead on
// arrival) has no record, stays vacuously available forever, and can never
// be flagged unreachable. This is exactly what TestSBRKeepOldestInterop
// and TestGoSeed_FailureRecovery exercise: the peer stops responding
// within the first heartbeat interval, and detection must still happen.
func TestHeartbeatTask_SeedsFDRecordWhenTargetNeverResponds(t *testing.T) {
	// Router swallows every send — the target never responds.
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.HeartbeatInterval = 50 * time.Millisecond
	cm.ExpectedResponseAfter = 100 * time.Millisecond

	// Use a port no other test heartbeats: the heartbeat-task registry is
	// package-level and keyed host:port, so a stale task for a shared port
	// (2551/2552) from an earlier test would keep this cm's task from
	// starting at all.
	r1 := makeUAWithDC("127.0.0.1", 39871, 51)
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberWithStatus(cm, r1, gproto_cluster.MemberStatus_Up, 2)
	cm.connectToNewMembers(cm.State)
	cm.Mu.Unlock()
	defer cm.StopHeartbeat(r1.GetAddress())

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hasFDRecord(cm, r1) {
			return // seeded — φ clock is running for the silent target
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no FD record was seeded for a monitored target that never responds — Pekko's TriggerFirstHeartbeat seeds the record expected-response-after into monitoring (ClusterHeartbeat.scala:257-259) so a dead-on-arrival member is still detectable")
}

// TestStartHeartbeat_IsolatedPerClusterManager pins that the heartbeat-task
// registry is scoped to ONE ClusterManager: a task started by another
// manager in the same process (integration suites run many nodes
// in-process; unit suites construct many managers) must not block this
// manager from monitoring the same host:port. With a shared package-level
// registry, whichever manager heartbeats an address first silently
// disables every later manager's monitoring of it — the later manager
// sends no heartbeats, seeds no FD record, and can never detect the
// peer's death (TestSBRKeepOldestInterop failed exactly this way inside
// the full -p 1 integration sequence while passing standalone).
func TestStartHeartbeat_IsolatedPerClusterManager(t *testing.T) {
	target := makeUAWithDC("127.0.0.1", 39872, 61)

	// First manager claims the target.
	router1 := func(_ context.Context, _ string, _ any) error { return nil }
	cm1 := NewClusterManager(makeUAWithDC("127.0.0.1", 39873, 62), router1)
	cm1.HeartbeatInterval = 50 * time.Millisecond
	cm1.StartHeartbeat(target.GetAddress())
	defer cm1.StopHeartbeat(target.GetAddress())

	// Second manager must still be able to monitor the same target.
	var mu sync.Mutex
	sent := 0
	router2 := func(_ context.Context, path string, msg any) error {
		if _, ok := msg.(*gproto_cluster.Heartbeat); ok {
			mu.Lock()
			sent++
			mu.Unlock()
		}
		return nil
	}
	cm2 := NewClusterManager(makeUAWithDC("127.0.0.1", 39874, 63), router2)
	cm2.HeartbeatInterval = 50 * time.Millisecond
	cm2.StartHeartbeat(target.GetAddress())
	defer cm2.StopHeartbeat(target.GetAddress())

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := sent
		mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("second ClusterManager sent no heartbeats to a target another manager already monitors — a process-global task registry silently disables FD monitoring for every manager after the first (no heartbeats → no record → peer death undetectable)")
}

// TestNewPhiAccrualFailureDetector_DefaultsPauseTo3s pins the wrapper's
// default acceptable-heartbeat-pause at Pekko's cluster reference value
// (pekko.cluster.failure-detector.acceptable-heartbeat-pause = 3 s).
func TestNewPhiAccrualFailureDetector_DefaultsPauseTo3s(t *testing.T) {
	fd := NewPhiAccrualFailureDetector(8.0, 1000)
	if got := fd.AcceptableHeartbeatPause(); got != 3*time.Second {
		t.Fatalf("default acceptable-heartbeat-pause = %v, want 3s (Pekko cluster reference default)", got)
	}
}

// TestConnectToNewMembers_RemovesFDRecordsForDeselectedTargets pins the
// record-removal half of Pekko's lifecycle: when a target drops out of the
// monitored heartbeat set (ring reshuffle after a membership change, or
// member removal), its per-node FD record must be REMOVED
// (ClusterHeartbeat.scala:287-290, 305-309) — keeping it means the record
// starves once heartbeating stops and CheckReachability false-flags the
// still-healthy node.
func TestConnectToNewMembers_RemovesFDRecordsForDeselectedTargets(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	r1 := makeUAWithDC("127.0.0.1", 2551, 51)
	r2 := makeUAWithDC("127.0.0.1", 2552, 52)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberWithStatus(cm, r1, gproto_cluster.MemberStatus_Up, 2)
	addMemberWithStatus(cm, r2, gproto_cluster.MemberStatus_Up, 3)
	cm.Mu.Unlock()

	// Monitor everything first: heartbeat tasks for both remotes.
	cm.Mu.Lock()
	cm.connectToNewMembers(cm.State)
	cm.Mu.Unlock()
	defer func() {
		cm.StopHeartbeat(r1.GetAddress())
		cm.StopHeartbeat(r2.GetAddress())
	}()

	// Simulate monitoring history for both targets.
	cm.Fd.Heartbeat(fdKeyFor(r1))
	cm.Fd.Heartbeat(fdKeyFor(r2))

	// Shrink the ring: only ONE target stays monitored. The sorted ring is
	// [2551, 2552] and the first key greater than the local 2541 is 2551,
	// so 2552 is deselected.
	cm.MonitoredByNrOfMembers = 1
	cm.Mu.Lock()
	cm.connectToNewMembers(cm.State)
	cm.Mu.Unlock()

	if !hasFDRecord(cm, r1) {
		t.Fatal("FD record for the still-monitored target was removed")
	}
	if hasFDRecord(cm, r2) {
		t.Fatal("FD record for the ring-deselected target survived — Pekko removes the record when a node leaves the monitored set (ClusterHeartbeat.scala:287-290, 305-309); a kept record starves once heartbeating stops and false-flags the healthy node")
	}
}

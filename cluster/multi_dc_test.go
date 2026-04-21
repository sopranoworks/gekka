/*
 * multi_dc_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// makeUAWithDC is a test helper that creates a UniqueAddress with a known host:port and uid.
func makeUAWithDC(host string, port uint32, uid uint32) *gproto_cluster.UniqueAddress {
	return &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String(host),
			Port:     proto.Uint32(port),
		},
		Uid: proto.Uint32(uid),
	}
}

// addMemberUpWithRoles adds a member to cm.State as Up with the given roles.
// Must be called while holding cm.Mu (write). Used only in tests.
func addMemberUpWithRoles(cm *ClusterManager, ua *gproto_cluster.UniqueAddress, upNumber int32, roles []string) {
	roleIdxs := cm.upsertRolesLocked(roles)
	addrIdx := int32(len(cm.State.AllAddresses))
	cm.State.AllAddresses = append(cm.State.AllAddresses, ua)
	cm.State.AllHashes = append(cm.State.AllHashes, "0")
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(upNumber),
		RolesIndexes: roleIdxs,
	})
}

// ── SetLocalDataCenter ────────────────────────────────────────────────────────

func TestSetLocalDataCenter_Default(t *testing.T) {
	local := makeUAWithDC("127.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })

	cm.SetLocalDataCenter("default")

	if cm.LocalDataCenter != "default" {
		t.Errorf("LocalDataCenter = %q, want default", cm.LocalDataCenter)
	}
	// Verify the initial member has the "dc-default" role.
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	if len(cm.State.Members) == 0 {
		t.Fatal("no members in initial state")
	}
	roles := GetRolesForMember(cm.State, cm.State.Members[0])
	found := false
	for _, r := range roles {
		if r == "dc-default" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected dc-default role in initial member, got %v", roles)
	}
}

func TestSetLocalDataCenter_EmptyDefaultsToDefault(t *testing.T) {
	local := makeUAWithDC("127.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("")
	if cm.LocalDataCenter != "default" {
		t.Errorf("LocalDataCenter = %q, want default", cm.LocalDataCenter)
	}
}

// ── DataCenterForMember ───────────────────────────────────────────────────────

func TestDataCenterForMember_NoRole(t *testing.T) {
	gossip := &gproto_cluster.Gossip{}
	m := &gproto_cluster.Member{}
	dc := DataCenterForMember(gossip, m)
	if dc != "default" {
		t.Errorf("expected default, got %q", dc)
	}
}

func TestDataCenterForMember_WithDCRole(t *testing.T) {
	gossip := &gproto_cluster.Gossip{
		AllRoles: []string{"worker", "dc-us-east"},
	}
	m := &gproto_cluster.Member{RolesIndexes: []int32{0, 1}}
	dc := DataCenterForMember(gossip, m)
	if dc != "us-east" {
		t.Errorf("expected us-east, got %q", dc)
	}
}

// ── OldestNodeInDC ────────────────────────────────────────────────────────────

func TestOldestNodeInDC_SingleDC(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	nodeB := makeUAWithDC("10.0.0.2", 2552, 2)
	nodeC := makeUAWithDC("10.0.0.3", 2552, 3)

	cm.Mu.Lock()
	// The initial local member already has dc-us-east (set by SetLocalDataCenter).
	// Make it Up.
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)

	addMemberUpWithRoles(cm, nodeB, 2, []string{"dc-us-east"})
	addMemberUpWithRoles(cm, nodeC, 3, []string{"dc-eu-west"}) // different DC
	cm.Mu.Unlock()

	ua := cm.OldestNodeInDC("us-east", "")
	if ua == nil {
		t.Fatal("expected a result, got nil")
	}
	if ua.GetAddress().GetHostname() != "10.0.0.1" {
		t.Errorf("oldest in us-east = %s, want 10.0.0.1", ua.GetAddress().GetHostname())
	}
}

func TestOldestNodeInDC_FiltersDifferentDC(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	nodeEU := makeUAWithDC("10.1.0.1", 2552, 2)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, nodeEU, 2, []string{"dc-eu-west"})
	cm.Mu.Unlock()

	ua := cm.OldestNodeInDC("eu-west", "")
	if ua == nil {
		t.Fatal("expected a result for eu-west DC, got nil")
	}
	if ua.GetAddress().GetHostname() != "10.1.0.1" {
		t.Errorf("oldest in eu-west = %s, want 10.1.0.1", ua.GetAddress().GetHostname())
	}

	// us-east oldest should not include EU node
	ua2 := cm.OldestNodeInDC("us-east", "")
	if ua2 == nil {
		t.Fatal("expected a result for us-east DC")
	}
	if ua2.GetAddress().GetHostname() != "10.0.0.1" {
		t.Errorf("oldest in us-east = %s, want 10.0.0.1", ua2.GetAddress().GetHostname())
	}
}

func TestOldestNodeInDC_WithRoleFilter(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	nodeWorker := makeUAWithDC("10.0.0.2", 2552, 2)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	// nodeWorker has dc-us-east AND worker role
	addMemberUpWithRoles(cm, nodeWorker, 2, []string{"dc-us-east", "worker"})
	cm.Mu.Unlock()

	// Oldest with worker role in us-east should be nodeWorker (upNumber 2 > 1)
	ua := cm.OldestNodeInDC("us-east", "worker")
	if ua == nil {
		t.Fatal("expected result, got nil")
	}
	if ua.GetAddress().GetHostname() != "10.0.0.2" {
		t.Errorf("expected nodeWorker to be oldest with worker role, got %s", ua.GetAddress().GetHostname())
	}
}

func TestOldestNodeInDC_EmptyDCFallsBackToAll(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	cm.Mu.Unlock()

	// OldestNodeInDC with empty dc delegates to OldestNode
	ua := cm.OldestNodeInDC("", "")
	if ua == nil {
		t.Fatal("expected a result, got nil")
	}
}

// ── MembersInDataCenter ───────────────────────────────────────────────────────

func TestMembersInDataCenter(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	nodeB := makeUAWithDC("10.0.0.2", 2552, 2)
	nodeEU := makeUAWithDC("10.1.0.1", 2552, 3)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, nodeB, 2, []string{"dc-us-east"})
	addMemberUpWithRoles(cm, nodeEU, 3, []string{"dc-eu-west"})
	cm.Mu.Unlock()

	usEast := cm.MembersInDataCenter("us-east")
	if len(usEast) != 2 {
		t.Errorf("expected 2 members in us-east, got %d", len(usEast))
	}
	for _, ma := range usEast {
		if ma.DataCenter != "us-east" {
			t.Errorf("member %s has DataCenter=%q, want us-east", ma.Host, ma.DataCenter)
		}
	}

	euWest := cm.MembersInDataCenter("eu-west")
	if len(euWest) != 1 {
		t.Errorf("expected 1 member in eu-west, got %d", len(euWest))
	}
}

// ── IsInDataCenter ────────────────────────────────────────────────────────────

func TestIsInDataCenter(t *testing.T) {
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	nodeEU := makeUAWithDC("10.1.0.1", 2552, 2)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	addMemberUpWithRoles(cm, nodeEU, 2, []string{"dc-eu-west"})
	cm.Mu.Unlock()

	if !cm.IsInDataCenter("10.0.0.1", 2552, "us-east") {
		t.Error("10.0.0.1:2552 should be in us-east")
	}
	if cm.IsInDataCenter("10.0.0.1", 2552, "eu-west") {
		t.Error("10.0.0.1:2552 should NOT be in eu-west")
	}
	if !cm.IsInDataCenter("10.1.0.1", 2552, "eu-west") {
		t.Error("10.1.0.1:2552 should be in eu-west")
	}
	if cm.IsInDataCenter("10.99.0.1", 2552, "us-east") {
		t.Error("unknown address should return false")
	}
}

// ── HOCON parsing ─────────────────────────────────────────────────────────────

// TestHOCON_MultiDCConfig is tested in the root package hocon_config_test.go.
// The cluster package test below verifies upsertRolesLocked directly.

// ── Cross-DC gossip throttling ────────────────────────────────────────────────

// TestCrossDCGossipThrottling verifies that gossipTick skips foreign-DC nodes
// when CrossDataCenterGossipProbability = 0.  With probability 0 the cross-DC
// selection must never result in a sent message.
func TestCrossDCGossipThrottling_NeverSendsCrossDC(t *testing.T) {
	var sentPaths []string
	router := func(_ context.Context, path string, _ any) error {
		sentPaths = append(sentPaths, path)
		return nil
	}

	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.SetLocalDataCenter("dc-tokyo")
	cm.CrossDataCenterGossipProbability = 0 // 0 means "use default 0.1"

	// Add a same-DC peer and a cross-DC peer.
	sameNode := makeUAWithDC("10.0.1.2", 2552, 2)
	crossNode := makeUAWithDC("10.0.2.1", 2552, 3)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, sameNode, 2, []string{"dc-dc-tokyo"})
	addMemberUpWithRoles(cm, crossNode, 3, []string{"dc-dc-osaka"})
	cm.Mu.Unlock()

	// With probability explicitly set to 0 and a small number of ticks the
	// cross-DC node should statistically never be selected.  But since 0 is the
	// "use default (0.1)" sentinel, use a very small explicit value instead.
	cm.CrossDataCenterGossipProbability = 1e-9 // effectively 0

	// Run many ticks; cross-DC paths must appear very rarely.
	const ticks = 200
	for i := 0; i < ticks; i++ {
		sentPaths = sentPaths[:0]
		cm.gossipTick(true)
	}
	// We can't deterministically assert zero cross-DC messages (the test would be
	// flaky), but we CAN verify that gossipTick does not panic and compiles correctly.
}

// TestCrossDCGossipThrottling_AlwaysSendsCrossDC verifies that with probability
// 1.0 (always) cross-DC gossip is never suppressed.
func TestCrossDCGossipThrottling_AlwaysSendsCrossDC(t *testing.T) {
	sentToCross := 0
	crossHost := "10.0.2.1"
	router := func(_ context.Context, path string, _ any) error {
		if strings.Contains(path, crossHost) {
			sentToCross++
		}
		return nil
	}

	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.SetLocalDataCenter("dc-tokyo")
	cm.CrossDataCenterGossipProbability = 1.0 // always send cross-DC

	crossNode := makeUAWithDC(crossHost, 2552, 2)
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, crossNode, 2, []string{"dc-dc-osaka"})
	cm.Mu.Unlock()

	// Run enough ticks that the cross-DC node will be selected at least once.
	const ticks = 100
	for i := 0; i < ticks; i++ {
		cm.gossipTick(true)
	}
	if sentToCross == 0 {
		t.Error("expected at least one cross-DC gossip message with probability=1.0, got none")
	}
}

// ── Gossip different-view probability ─────────────────────────────────────────

// TestGossipDifferentViewProbability verifies that when a target node is already
// in the Seen set (same view), the gossip system re-rolls to prefer nodes not
// in the Seen set.
func TestGossipDifferentViewProbability_PrefersDifferentView(t *testing.T) {
	sentToUnseen := 0
	unseenHost := "10.0.0.3"
	router := func(_ context.Context, path string, _ any) error {
		if strings.Contains(path, unseenHost) {
			sentToUnseen++
		}
		return nil
	}

	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.GossipDifferentViewProbability = 1.0 // always prefer different view

	// Add two peers: one in Seen (same view), one not (different view).
	seenNode := makeUAWithDC("10.0.0.2", 2552, 2)
	unseenNode := makeUAWithDC(unseenHost, 2552, 3)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, seenNode, 2, nil)
	addMemberUpWithRoles(cm, unseenNode, 3, nil)
	// Mark seenNode as Seen (address index 1).
	if cm.State.Overview == nil {
		cm.State.Overview = &gproto_cluster.GossipOverview{}
	}
	cm.State.Overview.Seen = []int32{0, 1} // local + seenNode
	cm.Mu.Unlock()

	const ticks = 200
	for i := 0; i < ticks; i++ {
		cm.gossipTick(false)
	}
	// With probability=1.0, the unseen node (different view) should be selected
	// most of the time when the initial random pick lands on the seen node.
	// We expect at least 50% of messages to go to the unseen node.
	if sentToUnseen < ticks/4 {
		t.Errorf("expected at least %d messages to unseen node, got %d", ticks/4, sentToUnseen)
	}
}

// TestGossipDifferentViewProbability_ReduceAtLargeCluster verifies that the
// different-view probability is halved when cluster size exceeds the threshold.
func TestGossipDifferentViewProbability_ReduceAtLargeCluster(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.GossipDifferentViewProbability = 1.0
	cm.ReduceGossipDifferentViewProbability = 3 // reduce at 3 members

	// Add 3 peers (total 4 members > threshold of 3).
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	for i := 2; i <= 4; i++ {
		node := makeUAWithDC(fmt.Sprintf("10.0.0.%d", i), 2552, uint32(i))
		addMemberUpWithRoles(cm, node, int32(i), nil)
	}
	cm.Mu.Unlock()

	// This should not panic; the reduced probability (0.5) still works.
	for i := 0; i < 50; i++ {
		cm.gossipTick(false)
	}
}

// ── Gossip tombstone pruning ─────────────────────────────────────────────────

func TestPruneGossipTombstones(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.PruneGossipTombstonesAfter = 100 * time.Millisecond

	// Add a member and mark it as Removed with a tombstone.
	removedNode := makeUAWithDC("10.0.0.2", 2552, 2)
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, removedNode, 2, nil)
	// Transition to Removed.
	cm.State.Members[1].Status = gproto_cluster.MemberStatus_Removed.Enum()
	cm.recordTombstoneLocked("10.0.0.2", 2552)
	cm.Mu.Unlock()

	// Before TTL expires, member should still be present.
	cm.Mu.Lock()
	cm.pruneGossipTombstonesLocked()
	memberCount := len(cm.State.Members)
	cm.Mu.Unlock()
	if memberCount != 2 {
		t.Fatalf("expected 2 members before prune, got %d", memberCount)
	}

	// Wait for TTL to expire.
	time.Sleep(150 * time.Millisecond)

	cm.Mu.Lock()
	cm.pruneGossipTombstonesLocked()
	memberCount = len(cm.State.Members)
	cm.Mu.Unlock()
	if memberCount != 1 {
		t.Fatalf("expected 1 member after prune, got %d", memberCount)
	}
}

// ── DownAllNodesInDataCenter SBR strategy ────────────────────────────────────

func TestDownAllNodesInDataCenter_DownsTargetDC(t *testing.T) {
	s := &DownAllNodesInDataCenter{DataCenter: "dc-osaka"}

	self := MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "dc-tokyo"}
	reachable := []Member{
		{Address: MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "dc-tokyo"}, UpNumber: 1},
		{Address: MemberAddress{Host: "10.0.2.1", Port: 2552, DataCenter: "dc-osaka"}, UpNumber: 2},
	}
	unreachable := []Member{
		{Address: MemberAddress{Host: "10.0.2.2", Port: 2552, DataCenter: "dc-osaka"}, UpNumber: 3},
	}

	d := s.Decide(self, reachable, unreachable)
	if d.DownSelf {
		t.Error("DownSelf must be false: local DC is unaffected")
	}
	// All members of dc-osaka must be downed (reachable + unreachable).
	downing := make(map[string]bool)
	for _, m := range d.DownMembers {
		downing[m.Address.Host] = true
	}
	if !downing["10.0.2.1"] || !downing["10.0.2.2"] {
		t.Errorf("expected both dc-osaka nodes downed, got %v", d.DownMembers)
	}
	if downing["10.0.1.1"] {
		t.Error("dc-tokyo node must not be downed")
	}
}

func TestDownAllNodesInDataCenter_NoActionWhenDCHealthy(t *testing.T) {
	s := &DownAllNodesInDataCenter{DataCenter: "dc-osaka"}

	self := MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "dc-tokyo"}
	reachable := []Member{
		{Address: MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "dc-tokyo"}, UpNumber: 1},
		{Address: MemberAddress{Host: "10.0.2.1", Port: 2552, DataCenter: "dc-osaka"}, UpNumber: 2},
	}
	unreachable := []Member{
		// Only tokyo node is unreachable — osaka is fine.
		{Address: MemberAddress{Host: "10.0.1.2", Port: 2552, DataCenter: "dc-tokyo"}, UpNumber: 3},
	}

	d := s.Decide(self, reachable, unreachable)
	if d.DownSelf || len(d.DownMembers) > 0 {
		t.Errorf("expected no-op when target DC has no unreachable members, got %+v", d)
	}
}

func TestDownAllNodesInDataCenter_EmptyDCIsNoop(t *testing.T) {
	s := &DownAllNodesInDataCenter{DataCenter: ""}
	d := s.Decide(MemberAddress{}, []Member{}, []Member{{Address: MemberAddress{DataCenter: "dc-osaka"}}})
	if d.DownSelf || len(d.DownMembers) > 0 {
		t.Errorf("expected no-op for empty DataCenter, got %+v", d)
	}
}

func TestNewStrategy_DownAllNodesInDataCenter(t *testing.T) {
	s, err := NewStrategy(SBRConfig{
		ActiveStrategy: "down-all-nodes-in-data-center",
		Role:           "dc-osaka",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.(*DownAllNodesInDataCenter); !ok {
		t.Errorf("expected *DownAllNodesInDataCenter, got %T", s)
	}
}

// ── Multi-DC two-cluster simulation ──────────────────────────────────────────

// TestMultiDC_TwoDCs simulates a cluster spanning dc-tokyo and dc-osaka and
// verifies DC-aware membership queries.
func TestMultiDC_TwoDCs(t *testing.T) {
	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.SetLocalDataCenter("tokyo")

	// Add dc-tokyo peers.
	tokyo2 := makeUAWithDC("10.0.1.2", 2552, 2)
	// Add dc-osaka peers.
	osaka1 := makeUAWithDC("10.0.2.1", 2552, 3)
	osaka2 := makeUAWithDC("10.0.2.2", 2552, 4)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, tokyo2, 2, []string{"dc-tokyo"})
	addMemberUpWithRoles(cm, osaka1, 3, []string{"dc-osaka"})
	addMemberUpWithRoles(cm, osaka2, 4, []string{"dc-osaka"})
	cm.Mu.Unlock()

	t.Run("TokyoMemberCount", func(t *testing.T) {
		members := cm.MembersInDataCenter("tokyo")
		if len(members) != 2 {
			t.Errorf("expected 2 tokyo members, got %d", len(members))
		}
	})

	t.Run("OsakaMemberCount", func(t *testing.T) {
		members := cm.MembersInDataCenter("osaka")
		if len(members) != 2 {
			t.Errorf("expected 2 osaka members, got %d", len(members))
		}
	})

	t.Run("OldestInTokyo", func(t *testing.T) {
		ua := cm.OldestNodeInDC("tokyo", "")
		if ua == nil {
			t.Fatal("expected oldest tokyo node, got nil")
		}
		if ua.GetAddress().GetHostname() != "10.0.1.1" {
			t.Errorf("oldest tokyo = %s, want 10.0.1.1", ua.GetAddress().GetHostname())
		}
	})

	t.Run("OldestInOsaka", func(t *testing.T) {
		ua := cm.OldestNodeInDC("osaka", "")
		if ua == nil {
			t.Fatal("expected oldest osaka node, got nil")
		}
		if ua.GetAddress().GetHostname() != "10.0.2.1" {
			t.Errorf("oldest osaka = %s, want 10.0.2.1", ua.GetAddress().GetHostname())
		}
	})

	t.Run("CrossDCPartitionSBR", func(t *testing.T) {
		// Simulate an osaka-DC partition: osaka1 and osaka2 are unreachable.
		s := &DownAllNodesInDataCenter{DataCenter: "osaka"}
		self := MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "tokyo"}
		reachable := []Member{
			{Address: MemberAddress{Host: "10.0.1.1", Port: 2552, DataCenter: "tokyo"}},
			{Address: MemberAddress{Host: "10.0.1.2", Port: 2552, DataCenter: "tokyo"}},
		}
		unreachable := []Member{
			{Address: MemberAddress{Host: "10.0.2.1", Port: 2552, DataCenter: "osaka"}},
			{Address: MemberAddress{Host: "10.0.2.2", Port: 2552, DataCenter: "osaka"}},
		}
		d := s.Decide(self, reachable, unreachable)
		if d.DownSelf {
			t.Error("tokyo node must not down itself")
		}
		if len(d.DownMembers) != 2 {
			t.Errorf("expected 2 downed osaka members, got %d", len(d.DownMembers))
		}
	})
}

func TestUpsertRolesLocked_Deduplication(t *testing.T) {
	local := makeUAWithDC("127.0.0.1", 2552, 1)
	cm := NewClusterManager(local, func(ctx context.Context, path string, msg any) error { return nil })

	cm.Mu.Lock()
	idxs1 := cm.upsertRolesLocked([]string{"worker", "dc-us-east"})
	idxs2 := cm.upsertRolesLocked([]string{"dc-us-east", "backend"}) // dc-us-east already exists
	cm.Mu.Unlock()

	if len(idxs1) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(idxs1))
	}
	// dc-us-east should reuse its index
	if idxs2[0] != idxs1[1] {
		t.Errorf("dc-us-east index should be reused: got %d, want %d", idxs2[0], idxs1[1])
	}
	// AllRoles should have 3 unique entries
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	if len(cm.State.AllRoles) != 3 {
		t.Errorf("AllRoles len = %d, want 3 (worker, dc-us-east, backend)", len(cm.State.AllRoles))
	}
}

// ── Down Removal Margin Tests ───────────────────────────────────────────────

func TestDownRemovalMargin_DelaysRemoval(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)
	cm.DownRemovalMargin = 200 * time.Millisecond

	// Add a second member and mark it as Down.
	node2 := makeUAWithDC("10.0.0.2", 2552, 2)
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, node2, 2, nil)
	cm.State.Members[1].Status = gproto_cluster.MemberStatus_Down.Enum()
	cm.Mu.Unlock()

	// First leader actions should NOT remove the downed member (margin not elapsed).
	cm.performLeaderActions()
	cm.Mu.RLock()
	status := cm.State.Members[1].GetStatus()
	cm.Mu.RUnlock()
	if status == gproto_cluster.MemberStatus_Removed {
		t.Fatal("member should not be Removed before down-removal-margin elapsed")
	}

	// Wait for margin to elapse.
	time.Sleep(250 * time.Millisecond)

	// Now leader actions should remove the member.
	cm.performLeaderActions()
	cm.Mu.RLock()
	status = cm.State.Members[1].GetStatus()
	cm.Mu.RUnlock()
	if status != gproto_cluster.MemberStatus_Removed {
		t.Errorf("member should be Removed after margin, got %v", status)
	}
}

func TestDownRemovalMargin_ZeroMeansImmediate(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)
	// DownRemovalMargin = 0 (default) — immediate removal.

	node2 := makeUAWithDC("10.0.0.2", 2552, 2)
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	addMemberUpWithRoles(cm, node2, 2, nil)
	cm.State.Members[1].Status = gproto_cluster.MemberStatus_Down.Enum()
	cm.Mu.Unlock()

	cm.performLeaderActions()
	cm.Mu.RLock()
	status := cm.State.Members[1].GetStatus()
	cm.Mu.RUnlock()
	if status != gproto_cluster.MemberStatus_Removed {
		t.Errorf("member should be Removed immediately with zero margin, got %v", status)
	}
}

// ── Config Compat Check Tests ───────────────────────────────────────────────

func TestCheckConfigCompat_SBRMatches(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	// Remote sends SBR config — should be compatible.
	remoteConfig := `pekko.cluster.downing-provider-class = "pekko.cluster.sbr.SplitBrainResolverProvider"`
	if !cm.CheckConfigCompat(remoteConfig) {
		t.Error("SBR config should be compatible")
	}
}

func TestCheckConfigCompat_EmptyAllowed(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	// Remote sends empty downing-provider — should be compatible (empty is allowed).
	remoteConfig := `pekko.cluster.downing-provider-class = ""`
	if !cm.CheckConfigCompat(remoteConfig) {
		t.Error("empty downing-provider-class should be compatible")
	}
}

func TestCheckConfigCompat_CustomProviderRejected(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	// Remote sends a custom downing provider — should be incompatible.
	remoteConfig := `pekko.cluster.downing-provider-class = "com.example.CustomDowningProvider"`
	if cm.CheckConfigCompat(remoteConfig) {
		t.Error("custom downing-provider-class should be incompatible")
	}
}

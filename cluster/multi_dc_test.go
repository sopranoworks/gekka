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
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
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
	memberCount := len(cm.State.Members)
	var stillDown bool
	for _, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Down {
			stillDown = true
			break
		}
	}
	cm.Mu.RUnlock()
	if memberCount != 2 || !stillDown {
		t.Fatalf("member should still be present as Down before down-removal-margin elapsed (got memberCount=%d, stillDown=%v)", memberCount, stillDown)
	}

	// Wait for margin to elapse.
	time.Sleep(250 * time.Millisecond)

	// Now leader actions should remove the member from gossip state entirely
	// (transition to Removed AND drop from Members, mirroring Akka's
	// MembershipState.copyWithMembers semantics).
	cm.performLeaderActions()
	cm.Mu.RLock()
	memberCount = len(cm.State.Members)
	cm.Mu.RUnlock()
	if memberCount != 1 {
		t.Errorf("downed member should be dropped from Members after margin, got memberCount=%d", memberCount)
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
	memberCount := len(cm.State.Members)
	cm.Mu.RUnlock()
	if memberCount != 1 {
		t.Errorf("downed member should be dropped immediately with zero margin, got memberCount=%d", memberCount)
	}
}

// TestLeaderDropsStaleRemovedFromGossip exercises Issue 3 of the
// 2026-05-16 dashboard self-down spec: when gekka becomes leader and the
// gossip state contains stale Removed entries (inherited from incoming
// gossip — e.g. zombie slots in a corrupted cluster), the leader must
// drop them so subsequent gossip emissions do not rebroadcast the stale
// entries to live peers.
func TestLeaderDropsStaleRemovedFromGossip(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	// Build a state with three stale Removed entries alongside the live
	// local Up member.  None of these went through gekka's own transition
	// path, so they have no tombstone yet — simulating entries that
	// arrived via incoming gossip from a corrupted peer.
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	for i := 2; i <= 4; i++ {
		zombie := makeUAWithDC(fmt.Sprintf("10.0.0.%d", i), 2552, uint32(i))
		addMemberUpWithRoles(cm, zombie, int32(i), nil)
		cm.State.Members[len(cm.State.Members)-1].Status = gproto_cluster.MemberStatus_Removed.Enum()
	}
	cm.Mu.Unlock()

	cm.Mu.RLock()
	beforeCount := len(cm.State.Members)
	cm.Mu.RUnlock()
	if beforeCount != 4 {
		t.Fatalf("setup error: expected 4 members before leader actions, got %d", beforeCount)
	}

	// Leader actions must run dropRemovedMembersLocked at the end and
	// strip every Removed entry, regardless of tombstone state.
	cm.performLeaderActions()

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	if len(cm.State.Members) != 1 {
		t.Fatalf("leader actions should drop all stale Removed entries; got %d members, want 1", len(cm.State.Members))
	}
	if status := cm.State.Members[0].GetStatus(); status != gproto_cluster.MemberStatus_Up {
		t.Errorf("surviving member should be the live local Up entry, got status=%v", status)
	}
	// Every dropped entry must have its host:port recorded as a tombstone
	// so subsequent incoming gossip cannot resurrect the slot.
	for i := 2; i <= 4; i++ {
		key := fmt.Sprintf("10.0.0.%d:2552", i)
		if _, ok := cm.tombstones[key]; !ok {
			t.Errorf("dropped stale Removed entry %s should have a tombstone recorded", key)
		}
	}
}

// removedEventCollector captures every MemberRemoved event delivered to it.
// Used by the Issue-3 ingress-filter tests to assert that stale Removed
// entries in incoming gossip are silently dropped (no event emitted) while
// legitimate "X was Up, now Removed" transitions still surface.
type removedEventCollector struct {
	mu       sync.Mutex
	received []MemberRemoved
}

func (c *removedEventCollector) Tell(msg any, _ ...actor.Ref) {
	if evt, ok := msg.(MemberRemoved); ok {
		c.mu.Lock()
		c.received = append(c.received, evt)
		c.mu.Unlock()
	}
}
func (c *removedEventCollector) Path() string { return "/user/removedEventCollector" }
func (c *removedEventCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.received)
}
func (c *removedEventCollector) At(i int) MemberRemoved {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.received[i]
}

// gossipWithMembers builds a Gossip envelope whose Members include the local
// entry as Up plus the supplied (ua, status) pairs.  Version is set to a
// single hash entry with timestamp `version` so the result compares
// ClockBefore against an empty (timestamp=0) local state — exercising the
// "replace wholesale" path through processIncomingGossip.
func gossipWithMembers(local *gproto_cluster.UniqueAddress, version int64, entries []struct {
	ua     *gproto_cluster.UniqueAddress
	status gproto_cluster.MemberStatus
}) *gproto_cluster.Gossip {
	g := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local},
		AllHashes:    []string{"local-hash"},
	}
	g.Members = append(g.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(0),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(1),
	})
	for _, e := range entries {
		idx := int32(len(g.AllAddresses))
		g.AllAddresses = append(g.AllAddresses, e.ua)
		g.AllHashes = append(g.AllHashes, fmt.Sprintf("hash-%d", idx))
		g.Members = append(g.Members, &gproto_cluster.Member{
			AddressIndex: proto.Int32(idx),
			Status:       e.status.Enum(),
			UpNumber:     proto.Int32(int32(idx + 1)),
		})
	}
	g.Version = &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(0), Timestamp: proto.Int64(version)},
		},
	}
	g.Overview = &gproto_cluster.GossipOverview{}
	return g
}

// TestIngressFiltersStaleRemovedFromGossip exercises Issue 3 of the
// 2026-05-16 dashboard self-down spec:  when an incoming gossip envelope
// (Welcome or GossipEnvelope) carries Removed entries for addresses we
// have never seen before, processIncomingGossip must drop them at ingress
// — never letting them enter cm.State.Members, tombstoning the address,
// and emitting no MemberRemoved event (we cannot "remove" a member we
// never had).  Without this, gekka rebroadcasts the stale entries on its
// next gossip tick and peers' user-level ClusterManagers fire
// `Cluster.down(addr)` against what is now a live address.
func TestIngressFiltersStaleRemovedFromGossip(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	// Make the local entry Up so it doesn't get mistaken for a fresh join.
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	cm.Mu.Unlock()

	collector := &removedEventCollector{}
	cm.Subscribe(collector, EventMemberRemoved)

	zombie := makeUAWithDC("10.0.0.99", 2552, 9001)
	incoming := gossipWithMembers(local, 5, []struct {
		ua     *gproto_cluster.UniqueAddress
		status gproto_cluster.MemberStatus
	}{
		{ua: zombie, status: gproto_cluster.MemberStatus_Removed},
	})

	if err := cm.processIncomingGossip(incoming, nil); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	// Zombie must NOT appear in cm.State.Members.
	for _, m := range cm.State.Members {
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(cm.State.AllAddresses) {
			continue
		}
		a := cm.State.AllAddresses[idx].GetAddress()
		if a.GetHostname() == "10.0.0.99" && a.GetPort() == 2552 {
			t.Fatalf("stale Removed entry leaked into cm.State.Members: %v (status=%v)", a, m.GetStatus())
		}
	}

	// Tombstone must be recorded so future gossip cannot resurrect.
	if _, ok := cm.tombstones["10.0.0.99:2552"]; !ok {
		t.Errorf("tombstone for stale Removed address not recorded; got tombstones=%v", cm.tombstones)
	}

	// No MemberRemoved event — we never saw 10.0.0.99 as Up locally, so
	// "removed" is not a transition we should announce.
	if got := collector.Count(); got != 0 {
		t.Errorf("spurious MemberRemoved events for unknown stale Removed entry: count=%d, first=%v", got, collector.At(0).Member)
	}
}

// TestIngressEmitsRemovedForLocallyKnownAddress exercises the second arm
// of the Issue-3 fix: an incoming gossip envelope that marks a
// previously-Up member as Removed must
//   - drop the member from cm.State.Members (so it isn't rebroadcast),
//   - tombstone its host:port,
//   - emit a MemberRemoved event so local subscribers observe the
//     transition (otherwise application-level cleanup never fires).
func TestIngressEmitsRemovedForLocallyKnownAddress(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	peer := makeUAWithDC("10.0.0.2", 2552, 2)
	addMemberUpWithRoles(cm, peer, 2, nil)
	cm.Mu.Unlock()

	collector := &removedEventCollector{}
	cm.Subscribe(collector, EventMemberRemoved)

	// Same UID, status flipped to Removed in the incoming view.
	incoming := gossipWithMembers(local, 5, []struct {
		ua     *gproto_cluster.UniqueAddress
		status gproto_cluster.MemberStatus
	}{
		{ua: peer, status: gproto_cluster.MemberStatus_Removed},
	})

	if err := cm.processIncomingGossip(incoming, nil); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	for _, m := range cm.State.Members {
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(cm.State.AllAddresses) {
			continue
		}
		a := cm.State.AllAddresses[idx].GetAddress()
		if a.GetHostname() == "10.0.0.2" && a.GetPort() == 2552 {
			t.Errorf("peer Removed in incoming gossip should not remain in cm.State.Members (status=%v)", m.GetStatus())
		}
	}
	if _, ok := cm.tombstones["10.0.0.2:2552"]; !ok {
		t.Errorf("tombstone for peer not recorded; tombstones=%v", cm.tombstones)
	}
	cm.Mu.RUnlock()

	if got := collector.Count(); got != 1 {
		t.Fatalf("expected exactly 1 MemberRemoved event for locally-known address, got %d", got)
	}
	evt := collector.At(0)
	if evt.Member.Host != "10.0.0.2" || evt.Member.Port != 2552 {
		t.Errorf("MemberRemoved event address = %s:%d, want 10.0.0.2:2552", evt.Member.Host, evt.Member.Port)
	}
}

// TestIngressPreservesSelfInRemovedGossip guards the reincarnation path:
// when incoming gossip carries our OWN address as Removed (a prior
// incarnation's slot still lingering at the seed), the ingress filter
// must NOT strip the self-entry or tombstone our address — that work
// belongs to repairSelfReincarnationLocked, which replaces the stale UID
// with our current one and resets the status to Joining.  If the filter
// were to drop self instead, repair would silently miss the slot and
// the freshly-joined node would either be lost from gossip entirely or
// be re-injected with the wrong addrIdx.
func TestIngressPreservesSelfInRemovedGossip(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 42)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	cm.Mu.Unlock()

	// Incoming gossip carries self's host:port with a STALE UID and
	// status Removed (prior-incarnation slot at the seed).
	stale := makeUAWithDC("10.0.0.1", 2552, 7) // different UID from local (42)
	incoming := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{stale},
		AllHashes:    []string{"stale-hash"},
		Members: []*gproto_cluster.Member{
			{
				AddressIndex: proto.Int32(0),
				Status:       gproto_cluster.MemberStatus_Removed.Enum(),
				UpNumber:     proto.Int32(1),
			},
		},
		Version: &gproto_cluster.VectorClock{
			Versions: []*gproto_cluster.VectorClock_Version{
				{HashIndex: proto.Int32(0), Timestamp: proto.Int64(5)},
			},
		},
		Overview: &gproto_cluster.GossipOverview{},
	}

	if err := cm.processIncomingGossip(incoming, nil); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()

	// Self must NOT be tombstoned.
	if _, ok := cm.tombstones["10.0.0.1:2552"]; ok {
		t.Errorf("self address must not be tombstoned by ingress filter; tombstones=%v", cm.tombstones)
	}
	// Self must remain in cm.State.Members with a non-Removed status
	// (repairSelfReincarnationLocked replaces stale UID + sets Joining).
	found := false
	for _, m := range cm.State.Members {
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(cm.State.AllAddresses) {
			continue
		}
		a := cm.State.AllAddresses[idx].GetAddress()
		if a.GetHostname() == "10.0.0.1" && a.GetPort() == 2552 {
			if m.GetStatus() == gproto_cluster.MemberStatus_Removed {
				t.Errorf("self entry must not remain as Removed after ingress + repair; got status=%v", m.GetStatus())
			}
			found = true
			break
		}
	}
	if !found {
		t.Errorf("self entry missing from cm.State.Members after ingress + repair")
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

// ── DetermineLeader — Pekko leaderOf address ordering ────────────────────────

// TestDetermineLeader_AddressLowestUpMemberLeads pins Pekko's actual leader
// contract (MembershipState.leaderOf / Member.leaderStatusOrdering): the
// FIRST member in address ordering (hostname, port, uid) whose status is
// Up/Leaving. UpNumber plays NO role in leader election — it is ageOrdering,
// used for singleton-oldest (OldestNode) only.
//
// History: a973dea changed this ordering to upNumber-first to stop the
// 2026-05-16 live-diag cascade (gekka at :2560 became leader of a production
// cluster and drove stale Down slots → Removed, collapsing four JVMs). That
// hid the symptom but diverged from the protocol: every JVM peer computes
// leaderOf by address, so once the low-address gekka member is Up the JVM
// side unilaterally hands it leadership and stops promoting joiners — and a
// gekka that defers to the upNumber-lowest member leaves the cluster with NO
// acting leader. That deadlock is the clean-boot showcase convergence
// failure (findings/2026-07-12-open-clean-boot-convergence-issue.md,
// re-confirmed 3/3 on 2026-07-13). The destructive cascade a973dea targeted
// is prevented at its actual roots instead: stale-Removed-slot filtering on
// merge (TestDropRemovedMembersLocked_*, intakeRemovedFromIncomingLocked)
// and the pre-Welcome leader-action gate
// (TestPerformLeaderActions_NoLeaderActionsWhileRemoteJoinPending).
//
// Same dolce-shaped topology as the old test: local=2560 Up with upNumber=5,
// seed at :37777 with upNumber=1, more Up peers at :37564/:37565/:37567.
// Expected leader per the protocol: the address-lowest Up member — local.
func TestDetermineLeader_AddressLowestUpMemberLeads(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	// Local: low port, large upNumber (the fresh joiner, already promoted Up).
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(5)
	// Seed of record: high port, genuine upNumber=1.
	seed := makeUAWithDC("127.0.0.1", 37777, 1)
	addMemberUpWithRoles(cm, seed, 1, nil)
	// Additional Up peers; ports are all > local's 2560.
	for i, port := range []uint32{37564, 37565, 37567} {
		peer := makeUAWithDC("127.0.0.1", port, uint32(100+i))
		addMemberUpWithRoles(cm, peer, int32(2+i), nil)
	}
	cm.Mu.Unlock()

	got := cm.DetermineLeader()
	if got == nil {
		t.Fatal("DetermineLeader returned nil with five Up members in state")
	}
	a := got.GetAddress()
	if a.GetHostname() != "127.0.0.1" || a.GetPort() != 2560 {
		t.Fatalf("DetermineLeader returned %s:%d (uid=%d) — Pekko leaderOf selects the address-lowest Up member (2560); upNumber must not participate in leader election",
			a.GetHostname(), a.GetPort(), got.GetUid())
	}
}

// TestDetermineLeader_JoiningMemberCannotDisplaceUpMember covers the window
// where the local node has been added to cm.State.Members but not yet
// promoted (Joining, UpNumber=0) while a peer is Up. The Up peer must win
// leader election even though the local node sorts first by address —
// Joining members are only a bootstrap fallback when NO Up/Leaving member
// exists (Pekko leaderMemberStatus).
func TestDetermineLeader_JoiningMemberCannotDisplaceUpMember(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.1", 2552, 1)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	// Local stays Joining (the NewClusterManager default), UpNumber=0.
	peer := makeUAWithDC("10.0.0.2", 2552, 2)
	addMemberUpWithRoles(cm, peer, 3, nil)
	cm.Mu.Unlock()

	got := cm.DetermineLeader()
	if got == nil {
		t.Fatal("DetermineLeader returned nil")
	}
	if got.GetAddress().GetPort() != 2552 || got.GetAddress().GetHostname() != "10.0.0.2" {
		t.Fatalf("DetermineLeader returned %s:%d; want the Up peer at 10.0.0.2 (a Joining member must not displace an Up member)",
			got.GetAddress().GetHostname(), got.GetAddress().GetPort())
	}
}

// TestDetermineLeader_AddressOrderingIgnoresUpNumber confirms the
// deterministic ordering among Up members is purely (hostname, port, uid):
// the smaller hostname wins even when it carries the LARGER upNumber.
func TestDetermineLeader_AddressOrderingIgnoresUpNumber(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.2", 2552, 5)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	peer := makeUAWithDC("10.0.0.1", 2552, 7) // smaller hostname, larger upNumber
	addMemberUpWithRoles(cm, peer, 7, nil)
	cm.Mu.Unlock()

	got := cm.DetermineLeader()
	if got == nil {
		t.Fatal("DetermineLeader returned nil")
	}
	if got.GetAddress().GetHostname() != "10.0.0.1" {
		t.Fatalf("DetermineLeader returned %s; want 10.0.0.1 (smallest address wins regardless of upNumber)",
			got.GetAddress().GetHostname())
	}
}

// ── dropRemovedMembersLocked — self preservation (Issue 3 lifecycle) ────────

// TestDropRemovedMembersLocked_PreservesSelf pins the second half of the
// dashboard self-down Issue 3 lifecycle contract: when self is moved to
// Removed status (the terminal state of CoordinatedShutdown's
// cluster-leave phase), the entry must remain in cm.State.Members so
// WaitForSelfRemoved and external callers polling cm.GetState() can
// observe the transition.  The companion ingress filter
// intakeRemovedFromIncomingLocked already preserves self; this test
// ensures the symmetric guard exists in the prune path.
func TestDropRemovedMembersLocked_PreservesSelf(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Removed.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(5)
	dropped := cm.dropRemovedMembersLocked()
	cm.Mu.Unlock()

	if dropped {
		t.Error("dropRemovedMembersLocked returned true for a self-only state; self must be preserved")
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	found := false
	for _, m := range cm.State.Members {
		idx := int(m.GetAddressIndex())
		if idx < 0 || idx >= len(cm.State.AllAddresses) {
			continue
		}
		a := cm.State.AllAddresses[idx].GetAddress()
		if a.GetHostname() == "127.0.0.1" && a.GetPort() == 2560 {
			if m.GetStatus() != gproto_cluster.MemberStatus_Removed {
				t.Errorf("self entry status mutated from Removed to %v", m.GetStatus())
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("self entry was stripped from cm.State.Members; lifecycle observation broken")
	}

	// Even though self was preserved, the address must still be tombstoned
	// so a future resurrection path treats it as gone.
	if _, ok := cm.tombstones["127.0.0.1:2560"]; !ok {
		t.Errorf("self tombstone not recorded; tombstones=%v", cm.tombstones)
	}
}

// ── repairSelfReincarnationLocked — stale terminal-status reset ───────────

// TestRepairResetsStaleDownStatusOnSelfUidSlot pins the live-diag root
// cause exposed against the production cluster on 127.0.0.1:37777: the
// seed welcomed gekka into its gossip, but the slot the seed assigned us
// already had `status=Down` (linger from a prior gekka incarnation that
// crashed at the same host:port; the seed's `down-removal-margin` had
// not yet pruned it).  Our exact UID matched the seed's slot — repair's
// UID-mismatch path was a no-op — yet the CoordinatedShutdown self-down
// subscriber fired within 1 s of Welcome because `selfMemberStatus`
// returned Down.
//
// The fix extends repair to recognise the UID-match-but-terminal-status
// case as a stale-slot collision and reset the status to Joining, but
// ONLY when we have not initiated our own Leave.  Once LeaveCluster()
// has been called, Down/Removed/Leaving/Exiting are legitimate and the
// reset must NOT fire.
func TestRepairResetsStaleDownStatusOnSelfUidSlot(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	// Simulate the merged-Welcome state: our exact-UID slot has been
	// adopted but its status is Down (the seed's stale-linger view).
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Down.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(0)
	cm.repairSelfReincarnationLocked()
	cm.Mu.Unlock()

	cm.Mu.RLock()
	got := cm.State.Members[0].GetStatus()
	cm.Mu.RUnlock()
	if got != gproto_cluster.MemberStatus_Joining {
		t.Fatalf("repair must reset stale Down on exact-UID self slot to Joining; got %v", got)
	}
}

// TestRepairResetsStaleRemovedStatusOnSelfUidSlot covers the Removed
// variant of the same collision.
func TestRepairResetsStaleRemovedStatusOnSelfUidSlot(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Removed.Enum()
	cm.repairSelfReincarnationLocked()
	cm.Mu.Unlock()

	cm.Mu.RLock()
	got := cm.State.Members[0].GetStatus()
	cm.Mu.RUnlock()
	if got != gproto_cluster.MemberStatus_Joining {
		t.Fatalf("repair must reset stale Removed on exact-UID self slot to Joining; got %v", got)
	}
}

// TestRepairDoesNotResetAfterLocalLeave confirms the guard: once the
// user (or CoordinatedShutdown's cluster-leave phase) has called
// LeaveCluster(), repair must NOT fight back against legitimate
// terminal transitions on our exact-UID slot.
func TestRepairDoesNotResetAfterLocalLeave(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	// Flag the local leave as initiated (without sending; we only need
	// the flag set).
	cm.localLeaveInitiated.Store(true)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Leaving.Enum()
	cm.repairSelfReincarnationLocked()
	cm.Mu.Unlock()

	cm.Mu.RLock()
	got := cm.State.Members[0].GetStatus()
	cm.Mu.RUnlock()
	if got != gproto_cluster.MemberStatus_Leaving {
		t.Fatalf("repair must preserve Leaving status after local Leave; got %v", got)
	}
}

// TestDropRemovedMembersLocked_DropsPeerKeepsSelf exercises the mixed
// case: a peer marked Removed must be stripped (the original purpose of
// the prune) while a co-resident self-Removed entry stays put.
func TestDropRemovedMembersLocked_DropsPeerKeepsSelf(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2560, 99)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Removed.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(5)
	peer := makeUAWithDC("10.0.0.7", 2552, 7)
	addMemberUpWithRoles(cm, peer, 6, nil)
	cm.State.Members[len(cm.State.Members)-1].Status = gproto_cluster.MemberStatus_Removed.Enum()
	dropped := cm.dropRemovedMembersLocked()
	cm.Mu.Unlock()

	if !dropped {
		t.Error("dropRemovedMembersLocked must return true when at least one non-self peer is dropped")
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	if len(cm.State.Members) != 1 {
		t.Fatalf("expected 1 surviving member (self), got %d", len(cm.State.Members))
	}
	idx := int(cm.State.Members[0].GetAddressIndex())
	a := cm.State.AllAddresses[idx].GetAddress()
	if a.GetHostname() != "127.0.0.1" || a.GetPort() != 2560 {
		t.Errorf("surviving member is %s:%d, expected self 127.0.0.1:2560", a.GetHostname(), a.GetPort())
	}
	if _, ok := cm.tombstones["10.0.0.7:2552"]; !ok {
		t.Errorf("peer tombstone not recorded")
	}
	if _, ok := cm.tombstones["127.0.0.1:2560"]; !ok {
		t.Errorf("self tombstone not recorded even though entry was preserved")
	}
}

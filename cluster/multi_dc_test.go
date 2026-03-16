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
	"testing"

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

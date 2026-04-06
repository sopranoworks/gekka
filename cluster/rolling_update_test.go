/*
 * rolling_update_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"testing"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"

	"google.golang.org/protobuf/proto"
)

// ── AppVersion type tests ───────────────────────────────────────────────────

func TestAppVersion_String(t *testing.T) {
	v := AppVersion{Major: 1, Minor: 2, Patch: 3}
	if s := v.String(); s != "1.2.3" {
		t.Errorf("got %q, want %q", s, "1.2.3")
	}
}

func TestAppVersion_Compare(t *testing.T) {
	tests := []struct {
		a, b AppVersion
		want int
	}{
		{AppVersion{1, 0, 0}, AppVersion{1, 0, 0}, 0},
		{AppVersion{1, 0, 0}, AppVersion{2, 0, 0}, -1},
		{AppVersion{2, 0, 0}, AppVersion{1, 0, 0}, 1},
		{AppVersion{1, 2, 0}, AppVersion{1, 1, 0}, 1},
		{AppVersion{1, 0, 1}, AppVersion{1, 0, 0}, 1},
	}
	for _, tt := range tests {
		got := tt.a.Compare(tt.b)
		if got != tt.want {
			t.Errorf("%v.Compare(%v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestParseAppVersion(t *testing.T) {
	v := ParseAppVersion("3.14.159")
	if v.Major != 3 || v.Minor != 14 || v.Patch != 159 {
		t.Errorf("parsed %v, want 3.14.159", v)
	}
	zero := ParseAppVersion("")
	if !zero.IsZero() {
		t.Errorf("empty string should parse to zero, got %v", zero)
	}
}

// ── Join handshake carries AppVersion ───────────────────────────────────────

func TestAppVersion_JoinCarriesVersion(t *testing.T) {
	// Create two ClusterManagers simulating two nodes.
	nodeA := newTestCMWithAddr("node-a", 2551)
	nodeA.SetLocalAppVersion(AppVersion{1, 0, 0})

	nodeB := newTestCMWithAddr("node-b", 2552)
	nodeB.SetLocalAppVersion(AppVersion{2, 0, 0})

	// Simulate nodeB joining nodeA: build a Join message as nodeB would.
	joinMsg := &gproto_cluster.Join{
		Node:  nodeB.LocalAddress,
		Roles: []string{"dc-default"},
	}
	v := nodeB.LocalAppVersion.String()
	joinMsg.AppVersion = &v

	// Marshal and unmarshal to simulate wire transfer.
	data, err := proto.Marshal(joinMsg)
	if err != nil {
		t.Fatal(err)
	}

	// nodeA handles the Join.
	join := &gproto_cluster.Join{}
	if err := proto.Unmarshal(data, join); err != nil {
		t.Fatal(err)
	}

	nodeA.Mu.Lock()
	nodeA.addMemberToGossipLocked(join.GetNode(), join.GetRoles(), join.GetAppVersion())
	nodeA.Mu.Unlock()

	// Verify nodeA's gossip state contains nodeB's AppVersion.
	nodeA.Mu.RLock()
	defer nodeA.Mu.RUnlock()

	if len(nodeA.State.AllAppVersions) < 2 {
		t.Fatalf("expected ≥2 app versions, got %v", nodeA.State.AllAppVersions)
	}

	// Find nodeB's member entry.
	found := false
	for _, m := range nodeA.State.Members {
		ua := nodeA.State.AllAddresses[m.GetAddressIndex()]
		if ua.GetAddress().GetHostname() == "node-b" {
			ver := nodeA.AppVersionForMember(m)
			if ver.String() != "2.0.0" {
				t.Errorf("nodeB version = %v, want 2.0.0", ver)
			}
			found = true
			break
		}
	}
	if !found {
		t.Error("nodeB not found in nodeA's gossip state")
	}
}

// ── SBR tiebreaker prefers higher AppVersion ────────────────────────────────

func TestSBR_KeepOldest_AppVersionTiebreaker(t *testing.T) {
	// Two members with same UpNumber but different versions.
	// The one with the higher version should be preferred as "oldest".
	oldVer := icluster.Member{
		Host:       "10.0.0.1",
		Port:       2551,
		UpNumber:   1,
		AppVersion: "1.0.0",
	}
	newVer := icluster.Member{
		Host:       "10.0.0.2",
		Port:       2551,
		UpNumber:   1,
		AppVersion: "2.0.0",
	}

	strategy := &icluster.KeepOldestStrategy{}

	// Scenario 1: newVer is reachable, oldVer is unreachable.
	// "oldest" with tiebreaker = newVer (higher AppVersion). It's reachable → Keep.
	action := strategy.Decide(
		[]icluster.Member{oldVer, newVer},
		[]icluster.Member{oldVer},
	)
	if action != icluster.Keep {
		t.Errorf("expected Keep when higher-version node is reachable, got %v", action)
	}

	// Scenario 2: newVer is unreachable, oldVer is reachable.
	// "oldest" = newVer (higher version). It's unreachable → Down.
	action = strategy.Decide(
		[]icluster.Member{oldVer, newVer},
		[]icluster.Member{newVer},
	)
	if action != icluster.Down {
		t.Errorf("expected Down when higher-version node is unreachable, got %v", action)
	}
}

func TestSBR_KeepOldest_UpNumberStillWins(t *testing.T) {
	// When UpNumber differs, it takes priority over AppVersion.
	older := icluster.Member{
		Host:       "10.0.0.1",
		Port:       2551,
		UpNumber:   1,
		AppVersion: "1.0.0",
	}
	newer := icluster.Member{
		Host:       "10.0.0.2",
		Port:       2551,
		UpNumber:   2,
		AppVersion: "99.0.0",
	}

	strategy := &icluster.KeepOldestStrategy{}

	// older (upNumber=1) is the "oldest" regardless of version.
	// older is reachable → Keep.
	action := strategy.Decide(
		[]icluster.Member{older, newer},
		[]icluster.Member{newer},
	)
	if action != icluster.Keep {
		t.Errorf("expected Keep (older UpNumber reachable), got %v", action)
	}
}

// ── Gossip merge preserves AppVersions ──────────────────────────────────────

func TestGossipMerge_AppVersions(t *testing.T) {
	cm := newTestCMWithAddr("node-a", 2551)
	cm.SetLocalAppVersion(AppVersion{1, 0, 0})

	// Build a second gossip state with a different node and version.
	s2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{
			{
				Address: &gproto_cluster.Address{
					Hostname: proto.String("node-b"),
					Port:     proto.Uint32(2552),
					Protocol: proto.String("pekko"),
					System:   proto.String("TestSystem"),
				},
				Uid: proto.Uint32(200),
			},
		},
		AllHashes:      []string{"200"},
		AllRoles:       []string{"dc-default"},
		AllAppVersions: []string{"3.0.0"},
		Members: []*gproto_cluster.Member{
			{
				AddressIndex:   proto.Int32(0),
				Status:         gproto_cluster.MemberStatus_Up.Enum(),
				UpNumber:       proto.Int32(2),
				RolesIndexes:   []int32{0},
				AppVersionIndex: proto.Int32(0),
			},
		},
		Version: &gproto_cluster.VectorClock{},
	}

	cm.Mu.Lock()
	merged := cm.mergeGossipStates(cm.State, s2)
	cm.Mu.Unlock()

	// Verify merged state has both app versions.
	if len(merged.AllAppVersions) < 2 {
		t.Fatalf("expected ≥2 AllAppVersions, got %v", merged.AllAppVersions)
	}

	// Find node-b member and check its version.
	for _, m := range merged.Members {
		ua := merged.AllAddresses[m.GetAddressIndex()]
		if ua.GetAddress().GetHostname() == "node-b" {
			idx := m.GetAppVersionIndex()
			if idx < 0 || int(idx) >= len(merged.AllAppVersions) {
				t.Fatalf("invalid AppVersionIndex %d for node-b", idx)
			}
			ver := merged.AllAppVersions[idx]
			if ver != "3.0.0" {
				t.Errorf("node-b version = %q, want %q", ver, "3.0.0")
			}
			return
		}
	}
	t.Error("node-b not found in merged state")
}

// ── AppVersionChanged event ─────────────────────────────────────────────────

func TestDiffGossipMembers_AppVersionChanged(t *testing.T) {
	addr := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Hostname: proto.String("10.0.0.1"),
			Port:     proto.Uint32(2551),
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
		},
		Uid: proto.Uint32(100),
	}

	oldState := &gproto_cluster.Gossip{
		AllAddresses:   []*gproto_cluster.UniqueAddress{addr},
		AllAppVersions: []string{"1.0.0"},
		Members: []*gproto_cluster.Member{
			{
				AddressIndex:   proto.Int32(0),
				Status:         gproto_cluster.MemberStatus_Up.Enum(),
				AppVersionIndex: proto.Int32(0),
			},
		},
	}

	newState := &gproto_cluster.Gossip{
		AllAddresses:   []*gproto_cluster.UniqueAddress{addr},
		AllAppVersions: []string{"1.0.0", "2.0.0"},
		Members: []*gproto_cluster.Member{
			{
				AddressIndex:   proto.Int32(0),
				Status:         gproto_cluster.MemberStatus_Up.Enum(),
				AppVersionIndex: proto.Int32(1), // changed from 0 to 1
			},
		},
	}

	events := diffGossipMembers(oldState, newState)

	foundVersionChange := false
	for _, evt := range events {
		if avc, ok := evt.(AppVersionChanged); ok {
			foundVersionChange = true
			if avc.OldVersion.String() != "1.0.0" {
				t.Errorf("OldVersion = %v, want 1.0.0", avc.OldVersion)
			}
			if avc.NewVersion.String() != "2.0.0" {
				t.Errorf("NewVersion = %v, want 2.0.0", avc.NewVersion)
			}
		}
	}
	if !foundVersionChange {
		t.Error("expected AppVersionChanged event, got none")
	}
}

// ── Helper ──────────────────────────────────────────────────────────────────

func newTestCMWithAddr(host string, port uint32) *ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Hostname: proto.String(host),
			Port:     proto.Uint32(port),
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
		},
		Uid: proto.Uint32(100),
	}
	return NewClusterManager(local, func(ctx context.Context, path string, msg any) error {
		return nil // no-op router for tests
	})
}

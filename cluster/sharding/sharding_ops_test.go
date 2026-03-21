/*
 * sharding_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

// Unit tests for manual shard operations (RebalanceShard control message).
//
// These tests drive the ShardCoordinator directly via Receive calls, bypassing
// the full actor runtime, to keep the test fast and deterministic.  The
// mock infrastructure from sharding_advanced_test.go is reused (same package).

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// newTestCoordinator wires a ShardCoordinator with a static metrics source and
// injects a mockRef as its Self() reference so that outgoing Tell calls
// (e.g. BeginHandOff) are captured.
func newTestCoordinator(t *testing.T) (*ShardCoordinator, *mockRef) {
	t.Helper()
	strategy := NewAdaptiveAllocationStrategy(
		&StaticMetricsReader{Loads: map[string]float64{}},
		0.5, 0.3, 2,
	)
	coord := NewShardCoordinator(strategy)
	selfRef := &mockRef{path: "/user/coordinator"}
	coord.SetSelf(selfRef)
	// Inject a mock actor context so that RegisterRegion's Watch call doesn't panic.
	mctx := newMockActorContext()
	actor.InjectSystem(coord, mctx)
	return coord, selfRef
}

// registerRegion simulates a ShardRegion registering with the coordinator.
func registerRegion(coord *ShardCoordinator, regionRef *mockRef) {
	actor.InjectSender(coord, regionRef)
	coord.Receive(RegisterRegion{RegionPath: regionRef.path})
	actor.InjectSender(coord, nil)
}

// allocateShard issues a GetShardHome and returns the assigned region path.
func allocateShard(coord *ShardCoordinator, requester *mockRef, shardID ShardId) string {
	actor.InjectSender(coord, requester)
	coord.Receive(GetShardHome{ShardId: shardID})
	actor.InjectSender(coord, nil)

	requester.mu.Lock()
	defer requester.mu.Unlock()
	for i := len(requester.messages) - 1; i >= 0; i-- {
		if h, ok := requester.messages[i].(ShardHome); ok && h.ShardId == shardID {
			return h.RegionPath
		}
	}
	return ""
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestRebalanceShard_MovesShardToTarget verifies the full manual-rebalance
// sequence:
//
//  1. Three regions register with the coordinator (nodeA, nodeB, nodeC).
//  2. Shard "s1" is allocated to nodeA via GetShardHome.
//  3. A RebalanceShard request moves "s1" to nodeC.
//  4. The coordinator sends BeginHandOff to nodeA.
//  5. nodeA acknowledges with BeginHandOffAck → coordinator sends HandOff.
//  6. nodeA completes drain with ShardStopped → coordinator pre-assigns "s1"
//     to nodeC.
//  7. AllocationSnapshot confirms shard "s1" is now on nodeC.
func TestRebalanceShard_MovesShardToTarget(t *testing.T) {
	coord, _ := newTestCoordinator(t)

	nodeA := &mockRef{path: "/user/region-A"}
	nodeB := &mockRef{path: "/user/region-B"}
	nodeC := &mockRef{path: "/user/region-C"}

	registerRegion(coord, nodeA)
	registerRegion(coord, nodeB)
	registerRegion(coord, nodeC)

	// Allocate shard s1 — should land on one of the regions (all have 0 shards;
	// AdaptiveAllocationStrategy with equal load picks whichever comes first in
	// the map iteration — we accept any, then verify the manual override).
	requester := &mockRef{path: "/user/region-A"}
	homePath := allocateShard(coord, requester, "s1")
	if homePath == "" {
		t.Fatal("GetShardHome returned no region")
	}

	// Force the shard to nodeA for a predictable starting point by directly
	// setting the internal map (only safe in same-package tests).
	coord.shards["s1"] = nodeA.path
	coord.updateSnapshot()

	// Verify initial allocation.
	snap := coord.AllocationSnapshot()
	if snap["s1"] != nodeA.path {
		t.Fatalf("pre-rebalance: expected s1 on %s, got %q", nodeA.path, snap["s1"])
	}

	// Step 3: send RebalanceShard to move s1 → nodeC.
	actor.InjectSender(coord, nil) // control messages have no sender
	coord.Receive(RebalanceShard{ShardId: "s1", TargetRegion: nodeC.path})

	// Step 4: coordinator must have sent BeginHandOff to nodeA.
	nodeA.mu.Lock()
	var gotBeginHandOff bool
	for _, m := range nodeA.messages {
		if bh, ok := m.(BeginHandOff); ok && bh.ShardId == "s1" {
			gotBeginHandOff = true
		}
	}
	nodeA.mu.Unlock()
	if !gotBeginHandOff {
		t.Fatal("expected BeginHandOff to be sent to nodeA")
	}

	// Step 5a: nodeA acknowledges BeginHandOff.
	actor.InjectSender(coord, nodeA)
	coord.Receive(BeginHandOffAck{ShardId: "s1"})
	actor.InjectSender(coord, nil)

	// Step 5b: coordinator must have sent HandOff to nodeA.
	nodeA.mu.Lock()
	var gotHandOff bool
	for _, m := range nodeA.messages {
		if h, ok := m.(HandOff); ok && h.ShardId == "s1" {
			gotHandOff = true
		}
	}
	nodeA.mu.Unlock()
	if !gotHandOff {
		t.Fatal("expected HandOff to be sent to nodeA")
	}

	// Step 6: nodeA signals shard drain complete.
	actor.InjectSender(coord, nodeA)
	coord.Receive(ShardStopped{ShardId: "s1"})
	actor.InjectSender(coord, nil)

	// Step 7: AllocationSnapshot must now show s1 → nodeC.
	snap = coord.AllocationSnapshot()
	if snap["s1"] != nodeC.path {
		t.Errorf("post-rebalance: expected s1 on %s, got %q", nodeC.path, snap["s1"])
	}
}

// TestRebalanceShard_UnknownShard verifies that RebalanceShard for a shard that
// has never been allocated is silently ignored (no BeginHandOff sent).
func TestRebalanceShard_UnknownShard(t *testing.T) {
	coord, _ := newTestCoordinator(t)
	nodeA := &mockRef{path: "/user/region-A"}
	registerRegion(coord, nodeA)

	coord.Receive(RebalanceShard{ShardId: "ghost", TargetRegion: nodeA.path})

	nodeA.mu.Lock()
	msgs := len(nodeA.messages)
	nodeA.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected no messages sent to nodeA, got %d", msgs)
	}
}

// TestRebalanceShard_UnknownTarget verifies that RebalanceShard for an
// unregistered target region is silently ignored.
func TestRebalanceShard_UnknownTarget(t *testing.T) {
	coord, _ := newTestCoordinator(t)
	nodeA := &mockRef{path: "/user/region-A"}
	registerRegion(coord, nodeA)
	coord.shards["s1"] = nodeA.path

	coord.Receive(RebalanceShard{ShardId: "s1", TargetRegion: "/user/region-ghost"})

	nodeA.mu.Lock()
	msgs := len(nodeA.messages)
	nodeA.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected no messages sent, got %d", msgs)
	}
}

// TestRebalanceShard_AlreadyOnTarget verifies that requesting a rebalance to
// the shard's current region is a no-op.
func TestRebalanceShard_AlreadyOnTarget(t *testing.T) {
	coord, _ := newTestCoordinator(t)
	nodeA := &mockRef{path: "/user/region-A"}
	registerRegion(coord, nodeA)
	coord.shards["s1"] = nodeA.path

	coord.Receive(RebalanceShard{ShardId: "s1", TargetRegion: nodeA.path})

	nodeA.mu.Lock()
	msgs := len(nodeA.messages)
	nodeA.mu.Unlock()
	if msgs != 0 {
		t.Errorf("expected no messages sent for no-op rebalance, got %d", msgs)
	}
}

// TestAdaptiveAllocationStrategy_PrefersLowLoadNode verifies that
// AdaptiveAllocationStrategy routes new shards to the region with the lowest
// composite score (high load-weight strongly favours the low-load node).
func TestAdaptiveAllocationStrategy_PrefersLowLoadNode(t *testing.T) {
	metrics := &StaticMetricsReader{Loads: map[string]float64{
		"/user/region-heavy": 0.9,
		"/user/region-light": 0.1,
	}}
	strategy := NewAdaptiveAllocationStrategy(metrics, 0.8, 0.2, 1)

	heavyRef := &mockRef{path: "/user/region-heavy"}
	lightRef := &mockRef{path: "/user/region-light"}

	allocations := map[actor.Ref][]ShardId{
		heavyRef: {"s0", "s1"},
		lightRef: {"s2"},
	}

	chosen := strategy.AllocateShard(nil, "s3", allocations)
	if chosen == nil {
		t.Fatal("AllocateShard returned nil")
	}
	if chosen.Path() != lightRef.path {
		t.Errorf("expected light region, got %q", chosen.Path())
	}
}

// TestAdaptiveAllocationStrategy_RebalanceTriggersOnHighSpread verifies that
// Rebalance proposes a shard to move when the score spread exceeds threshold.
func TestAdaptiveAllocationStrategy_RebalanceTriggersOnHighSpread(t *testing.T) {
	metrics := &StaticMetricsReader{Loads: map[string]float64{
		"/user/region-A": 0.9,
		"/user/region-B": 0.1,
	}}
	// threshold=0.2 → a spread >0.2 should trigger rebalance.
	strategy := NewAdaptiveAllocationStrategy(metrics, 0.8, 0.2, 2)

	refA := &mockRef{path: "/user/region-A"}
	refB := &mockRef{path: "/user/region-B"}

	allocations := map[actor.Ref][]ShardId{
		refA: {"s0", "s1", "s2"},
		refB: {},
	}
	toMove := strategy.Rebalance(allocations, nil)
	if len(toMove) == 0 {
		t.Error("expected Rebalance to propose a shard to move")
	}
}

// TestAdaptiveAllocationStrategy_NoRebalanceWhenBalanced verifies that
// Rebalance returns nil when the score spread is within threshold.
func TestAdaptiveAllocationStrategy_NoRebalanceWhenBalanced(t *testing.T) {
	metrics := &StaticMetricsReader{Loads: map[string]float64{
		"/user/region-A": 0.5,
		"/user/region-B": 0.5,
	}}
	strategy := NewAdaptiveAllocationStrategy(metrics, 0.5, 0.3, 2)

	refA := &mockRef{path: "/user/region-A"}
	refB := &mockRef{path: "/user/region-B"}

	allocations := map[actor.Ref][]ShardId{
		refA: {"s0", "s1"},
		refB: {"s2", "s3"},
	}
	toMove := strategy.Rebalance(allocations, nil)
	if len(toMove) != 0 {
		t.Errorf("expected no rebalance, got %v", toMove)
	}
}

/*
 * consistent_hashing_strategy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// mockRegionRef is a minimal actor.Ref implementation for testing.
type mockRegionRef struct{ name string }

func (r *mockRegionRef) Tell(msg any, sender ...actor.Ref) {}
func (r *mockRegionRef) Path() string                      { return "/user/" + r.name }

func TestConsistentHashingAllocation_DeterministicMapping(t *testing.T) {
	strategy := NewConsistentHashingAllocationStrategy(100)

	r1 := &mockRegionRef{name: "region-1"}
	r2 := &mockRegionRef{name: "region-2"}
	r3 := &mockRegionRef{name: "region-3"}

	allocations := map[actor.Ref][]ShardId{
		r1: {},
		r2: {},
		r3: {},
	}

	shardId := ShardId("shard-42")

	// Allocate the same shard 10 times — must always land on the same region.
	first := strategy.AllocateShard(r1, shardId, allocations)
	if first == nil {
		t.Fatal("AllocateShard returned nil")
	}
	for i := 1; i < 10; i++ {
		got := strategy.AllocateShard(r1, shardId, allocations)
		if got.Path() != first.Path() {
			t.Fatalf("iteration %d: expected %s, got %s", i, first.Path(), got.Path())
		}
	}
}

func TestConsistentHashingAllocation_MinimalMovement(t *testing.T) {
	strategy := NewConsistentHashingAllocationStrategy(100)

	r1 := &mockRegionRef{name: "region-A"}
	r2 := &mockRegionRef{name: "region-B"}
	r3 := &mockRegionRef{name: "region-C"}

	// Phase 1: allocate 20 shards across 2 regions.
	alloc2 := map[actor.Ref][]ShardId{
		r1: {},
		r2: {},
	}
	phase1 := make(map[ShardId]string) // shardId -> region path
	for i := 0; i < 20; i++ {
		sid := ShardId(fmt.Sprintf("shard-%d", i))
		region := strategy.AllocateShard(r1, sid, alloc2)
		phase1[sid] = region.Path()
	}

	// Phase 2: add a 3rd region, re-allocate.
	alloc3 := map[actor.Ref][]ShardId{
		r1: {},
		r2: {},
		r3: {},
	}
	moved := 0
	for i := 0; i < 20; i++ {
		sid := ShardId(fmt.Sprintf("shard-%d", i))
		region := strategy.AllocateShard(r1, sid, alloc3)
		if region.Path() != phase1[sid] {
			moved++
		}
	}

	// With consistent hashing, adding 1 of 3 nodes should move roughly 1/3 of
	// shards.  We allow up to 70% to account for hash variance.
	maxAllowed := 14 // 70% of 20
	if moved > maxAllowed {
		t.Fatalf("too many shards moved: %d out of 20 (max allowed %d)", moved, maxAllowed)
	}
	t.Logf("shards moved after adding 3rd region: %d/20", moved)
}

func TestConsistentHashingAllocation_Rebalance(t *testing.T) {
	strategy := NewConsistentHashingAllocationStrategy(100)

	r1 := &mockRegionRef{name: "node-1"}
	r2 := &mockRegionRef{name: "node-2"}

	// Determine where the ring would place each shard.
	ideal := map[actor.Ref][]ShardId{
		r1: {},
		r2: {},
	}
	idealOwner := make(map[ShardId]string)
	for i := 0; i < 10; i++ {
		sid := ShardId(fmt.Sprintf("s%d", i))
		region := strategy.AllocateShard(r1, sid, ideal)
		idealOwner[sid] = region.Path()
	}

	// Now put ALL shards on r1, even those that should go to r2.
	allOnR1 := make([]ShardId, 0, 10)
	for i := 0; i < 10; i++ {
		allOnR1 = append(allOnR1, ShardId(fmt.Sprintf("s%d", i)))
	}
	wrongAllocations := map[actor.Ref][]ShardId{
		r1: allOnR1,
		r2: {},
	}

	// Count how many are actually on the wrong region.
	wrongCount := 0
	for _, sid := range allOnR1 {
		if idealOwner[sid] != r1.Path() {
			wrongCount++
		}
	}

	if wrongCount == 0 {
		t.Skip("all shards happened to hash to r1; cannot test rebalance")
	}

	// Rebalance should return exactly 1 shard to move.
	toMove := strategy.Rebalance(wrongAllocations, nil)
	if len(toMove) != 1 {
		t.Fatalf("expected 1 shard to rebalance, got %d", len(toMove))
	}

	// The shard returned must be one that is on the wrong region.
	if idealOwner[toMove[0]] == r1.Path() {
		t.Fatalf("Rebalance returned shard %s which is already on its ideal region", toMove[0])
	}

	// Rebalance should return nil when rebalanceInProgress is non-empty.
	toMove2 := strategy.Rebalance(wrongAllocations, []ShardId{"s0"})
	if toMove2 != nil {
		t.Fatalf("expected nil when rebalanceInProgress is non-empty, got %v", toMove2)
	}
}

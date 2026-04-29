/*
 * strategy_v2_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"sort"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

func mkShards(prefix string, n int) []ShardId {
	out := make([]ShardId, n)
	for i := 0; i < n; i++ {
		out[i] = ShardId(prefix + "-" + string(rune('a'+i)))
	}
	return out
}

func sortIds(ids []ShardId) []ShardId {
	out := make([]ShardId, len(ids))
	copy(out, ids)
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out
}

// Phase 1: a region with more shards than optimal donates the excess; cap is
// not hit, so the full excess shows up.
func TestLeastShardAllocationStrategyV2_Phase1_TrimsToOptimal(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}
	r3 := &mockRegionRef{name: "r3"}

	// 9 shards across 3 regions: optimal = 3. r1 holds 7 → 4 excess.
	// relativeLimit=1.0 means cap == total == 9, so excess (4) flows untrimmed.
	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 7),
		r2: {ShardId("b-a")},
		r3: {ShardId("c-a")},
	}

	s := NewLeastShardAllocationStrategyV2(10, 1.0)
	got := s.Rebalance(alloc, nil)

	// Expect 4 shards (the head 4 of r1's allocation).
	if len(got) != 4 {
		t.Fatalf("phase1: want 4 shards, got %d (%v)", len(got), got)
	}
}

// Phase 1 cap: limit truncates the excess to absoluteLimit.
func TestLeastShardAllocationStrategyV2_Phase1_CapByAbsoluteLimit(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}

	// 10 shards on r1, 0 on r2. Optimal = 5. Excess = 5 — but absoluteLimit=2
	// caps the result.
	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 10),
		r2: {},
	}

	s := NewLeastShardAllocationStrategyV2(2, 1.0)
	got := s.Rebalance(alloc, nil)
	if len(got) != 2 {
		t.Fatalf("phase1 absolute cap: want 2 shards, got %d", len(got))
	}
}

// Phase 1 cap: relativeLimit*total is smaller than absoluteLimit.
func TestLeastShardAllocationStrategyV2_Phase1_CapByRelativeLimit(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}

	// 20 shards total, r1 has 20. relative=0.1 → 2; absolute=10. min=2.
	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 20),
		r2: {},
	}

	s := NewLeastShardAllocationStrategyV2(10, 0.1)
	got := s.Rebalance(alloc, nil)
	if len(got) != 2 {
		t.Fatalf("phase1 relative cap: want 2 shards, got %d", len(got))
	}
}

// Phase 2 fires only when phase 1 is empty AND a region is ≥ 2 below optimal.
func TestLeastShardAllocationStrategyV2_Phase2_LevelsOutWhenBelowOptimal(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}
	r3 := &mockRegionRef{name: "r3"}

	// 5 shards, 3 regions: optimal = 2.
	// r1=2, r2=2, r3=1. Phase 1 empty. r3 is 1 below optimal which is < 2 below
	// optimal-1. countBelow = max(0, 1 - 1) = 0 → phase2 returns nothing.
	alloc := map[actor.Ref][]ShardId{
		r1: {ShardId("a-1"), ShardId("a-2")},
		r2: {ShardId("b-1"), ShardId("b-2")},
		r3: {ShardId("c-1")},
	}
	s := NewLeastShardAllocationStrategyV2(10, 1.0)
	got := s.Rebalance(alloc, nil)
	if len(got) != 0 {
		t.Fatalf("balanced enough: want 0, got %v", got)
	}

	// Now make r3 empty: optimal=2, r3=0 → countBelow = (2-1)-0 = 1.
	// r1 and r2 are ≥ optimal, so each donates 1 head shard. Result limited by
	// min(countBelow=1, cap=10) = 1.
	alloc[r3] = nil
	got = s.Rebalance(alloc, nil)
	if len(got) != 1 {
		t.Fatalf("phase2: want 1 shard, got %d (%v)", len(got), got)
	}
}

// Rebalance returns nil when a rebalance is already in progress, regardless of
// configuration.
func TestLeastShardAllocationStrategyV2_NoOpWhenRebalanceInProgress(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}

	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 5),
		r2: {},
	}

	s := NewLeastShardAllocationStrategyV2(10, 1.0)
	got := s.Rebalance(alloc, []ShardId{ShardId("a-x")})
	if got != nil {
		t.Fatalf("in-progress: want nil, got %v", got)
	}
}

// AllocateShard prefers the region with the fewest shards.
func TestLeastShardAllocationStrategyV2_Allocate_PrefersLeastLoaded(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}
	r3 := &mockRegionRef{name: "r3"}

	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 5),
		r2: mkShards("b", 1),
		r3: mkShards("c", 3),
	}

	s := NewLeastShardAllocationStrategyV2(10, 0.1)
	got := s.AllocateShard(r1, ShardId("z"), alloc)
	if got == nil || got.Path() != r2.Path() {
		t.Fatalf("allocate: want %s, got %v", r2.Path(), got)
	}
}

// Determinism: identical input yields identical output across runs (the
// internal sort orders entries by descending size, then by region path).
func TestLeastShardAllocationStrategyV2_DeterministicOrder(t *testing.T) {
	r1 := &mockRegionRef{name: "r1"}
	r2 := &mockRegionRef{name: "r2"}
	r3 := &mockRegionRef{name: "r3"}

	alloc := map[actor.Ref][]ShardId{
		r1: mkShards("a", 6),
		r2: mkShards("b", 1),
		r3: mkShards("c", 1),
	}

	s := NewLeastShardAllocationStrategyV2(10, 1.0)

	first := s.Rebalance(alloc, nil)
	for i := 0; i < 5; i++ {
		next := s.Rebalance(alloc, nil)
		if len(first) != len(next) {
			t.Fatalf("non-deterministic length on iter %d", i)
		}
		a, b := sortIds(first), sortIds(next)
		for j := range a {
			if a[j] != b[j] {
				t.Fatalf("non-deterministic content on iter %d: %v vs %v", i, first, next)
			}
		}
	}
}

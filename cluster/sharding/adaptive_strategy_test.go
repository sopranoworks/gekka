/*
 * adaptive_strategy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

func TestAdaptiveAllocationStrategy_AllocateShard(t *testing.T) {
	metrics := &StaticMetricsReader{
		Loads: map[string]float64{
			"/user/region1": 0.8,
			"/user/region2": 0.2,
		},
	}

	// loadWeight=1.0: pure load-based
	strategy := NewAdaptiveAllocationStrategy(metrics, 1.0, 0.2, 1)

	r1 := &actor.FunctionalMockRef{PathURI: "/user/region1"}
	r2 := &actor.FunctionalMockRef{PathURI: "/user/region2"}

	allocs := map[actor.Ref][]ShardId{
		r1: {"s1", "s2"},
		r2: {"s3"},
	}

	// Should allocate to region2 (lower load)
	best := strategy.AllocateShard(nil, "s4", allocs)
	assert.Equal(t, r2.Path(), best.Path())

	// loadWeight=0.0: pure shard-count based (LeastShard)
	strategy0 := NewAdaptiveAllocationStrategy(metrics, 0.0, 0.2, 1)
	best0 := strategy0.AllocateShard(nil, "s4", allocs)
	assert.Equal(t, r2.Path(), best0.Path())

	// If region2 has more shards but much lower load
	allocs2 := map[actor.Ref][]ShardId{
		r1: {"s1"},
		r2: {"s2", "s3", "s4"},
	}
	// With loadWeight=0.5, region1 might win if shard-count is heavily weighted
	// score(r1) = 0.5*0.8 + 0.5*(1/4) = 0.4 + 0.125 = 0.525
	// score(r2) = 0.5*0.2 + 0.5*(3/4) = 0.1 + 0.375 = 0.475
	// r2 still wins because load difference is large
	best2 := strategy.AllocateShard(nil, "s5", allocs2)
	assert.Equal(t, r2.Path(), best2.Path())
}

func TestAdaptiveAllocationStrategy_Rebalance(t *testing.T) {
	metrics := &StaticMetricsReader{
		Loads: map[string]float64{
			"/user/region1": 0.9,
			"/user/region2": 0.1,
		},
	}

	// rebalanceThreshold=0.2, loadWeight=1.0
	strategy := NewAdaptiveAllocationStrategy(metrics, 1.0, 0.2, 2)

	r1 := &actor.FunctionalMockRef{PathURI: "/user/region1"}
	r2 := &actor.FunctionalMockRef{PathURI: "/user/region2"}

	allocs := map[actor.Ref][]ShardId{
		r1: {"s1", "s2", "s3"},
		r2: {"s4"},
	}

	// minScore=0.1, maxScore=0.9, spread=0.8 > 0.2. Should rebalance.
	// maxSimultaneousRebalance=2, inProgress=0. Should return 2 shards.
	toMove := strategy.Rebalance(allocs, nil)
	assert.Equal(t, 2, len(toMove))
	assert.Subset(t, []ShardId{"s1", "s2", "s3"}, toMove)

	// If one is already in progress, should return 1 more.
	toMove2 := strategy.Rebalance(allocs, []ShardId{"s1"})
	assert.Equal(t, 1, len(toMove2))
	assert.NotEqual(t, "s1", toMove2[0])
}

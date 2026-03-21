/*
 * strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"github.com/sopranoworks/gekka/actor"
)

// ── MetricsReader ─────────────────────────────────────────────────────────────

// MetricsReader supplies per-node load metrics to AdaptiveAllocationStrategy.
//
// Implementations typically read CPU and memory usage from the cluster-wide
// Distributed Data LWWMap (keyed as "node:<regionPath>:cpu" etc.), but any
// source is acceptable — including a static map for testing.
type MetricsReader interface {
	// NodeLoad returns a combined load score in [0.0, 1.0] for the region
	// identified by regionPath.  Lower is better.
	// When metrics are unavailable the implementation should return 0.5 so
	// the strategy gracefully falls back to shard-count tiebreaking.
	NodeLoad(regionPath string) float64
}

// StaticMetricsReader is a MetricsReader backed by a fixed map.
// Useful for unit tests and benchmarks.
type StaticMetricsReader struct {
	Loads map[string]float64 // regionPath → load score [0.0, 1.0]
}

// NodeLoad returns the pre-configured load for regionPath, or 0.5 as default.
func (s *StaticMetricsReader) NodeLoad(regionPath string) float64 {
	if v, ok := s.Loads[regionPath]; ok {
		return v
	}
	return 0.5
}

// ── AdaptiveAllocationStrategy ────────────────────────────────────────────────

// AdaptiveAllocationStrategy allocates shards to the region with the lowest
// combined score:
//
//	score = loadWeight * nodeLoad(region) + (1-loadWeight) * normalised_shard_count
//
// loadWeight controls how much the live metrics influence placement versus the
// raw shard-count (LeastShardAllocationStrategy uses loadWeight=0).
//
// When all nodes have equal load the strategy behaves identically to
// LeastShardAllocationStrategy.  Rebalancing triggers when the score difference
// between the most and least loaded region exceeds rebalanceThreshold.
type AdaptiveAllocationStrategy struct {
	metrics                  MetricsReader
	loadWeight               float64 // [0.0, 1.0]; 0 = pure shard-count, 1 = pure load
	rebalanceThreshold       float64 // minimum score spread that triggers rebalance
	maxSimultaneousRebalance int
}

// NewAdaptiveAllocationStrategy creates an AdaptiveAllocationStrategy.
//
//   - metrics                  — live node load source (use StaticMetricsReader for tests)
//   - loadWeight               — blend factor [0.0, 1.0] between load and shard-count
//   - rebalanceThreshold       — score spread that triggers a rebalance (e.g. 0.2)
//   - maxSimultaneousRebalance — cap on shards being moved at once
func NewAdaptiveAllocationStrategy(
	metrics MetricsReader,
	loadWeight float64,
	rebalanceThreshold float64,
	maxSimultaneousRebalance int,
) *AdaptiveAllocationStrategy {
	if loadWeight < 0 {
		loadWeight = 0
	}
	if loadWeight > 1 {
		loadWeight = 1
	}
	return &AdaptiveAllocationStrategy{
		metrics:                  metrics,
		loadWeight:               loadWeight,
		rebalanceThreshold:       rebalanceThreshold,
		maxSimultaneousRebalance: maxSimultaneousRebalance,
	}
}

// score computes the combined placement score for a region.
// A lower score makes the region a better candidate.
func (s *AdaptiveAllocationStrategy) score(ref actor.Ref, shardCount, totalShards int) float64 {
	load := s.metrics.NodeLoad(ref.Path())

	// Normalised shard fraction: 0.0 when region has no shards, 1.0 when it
	// holds all shards.  Guard against division by zero.
	var shardFraction float64
	if totalShards > 0 {
		shardFraction = float64(shardCount) / float64(totalShards)
	}

	return s.loadWeight*load + (1-s.loadWeight)*shardFraction
}

// AllocateShard returns the region with the lowest composite score.
func (s *AdaptiveAllocationStrategy) AllocateShard(
	_ actor.Ref,
	_ ShardId,
	currentShardAllocations map[actor.Ref][]ShardId,
) actor.Ref {
	totalShards := 0
	for _, shards := range currentShardAllocations {
		totalShards += len(shards)
	}

	var bestRegion actor.Ref
	bestScore := -1.0
	for region, shards := range currentShardAllocations {
		sc := s.score(region, len(shards), totalShards)
		if bestScore < 0 || sc < bestScore {
			bestScore = sc
			bestRegion = region
		}
	}
	return bestRegion
}

// Rebalance returns at most one shard to move from the highest-scored region
// toward balance.  Returns nil when the system is already sufficiently balanced
// or the in-progress cap is reached.
func (s *AdaptiveAllocationStrategy) Rebalance(
	currentShardAllocations map[actor.Ref][]ShardId,
	rebalanceInProgress []ShardId,
) []ShardId {
	if len(rebalanceInProgress) >= s.maxSimultaneousRebalance {
		return nil
	}

	totalShards := 0
	for _, shards := range currentShardAllocations {
		totalShards += len(shards)
	}

	minScore, maxScore := -1.0, -1.0
	var maxRegion actor.Ref

	for region, shards := range currentShardAllocations {
		sc := s.score(region, len(shards), totalShards)
		if minScore < 0 || sc < minScore {
			minScore = sc
		}
		if maxScore < 0 || sc > maxScore {
			maxScore = sc
			maxRegion = region
		}
	}

	if maxScore-minScore <= s.rebalanceThreshold {
		return nil
	}

	// Pick one shard from the most loaded region that is not already moving.
	inProgressSet := make(map[ShardId]struct{}, len(rebalanceInProgress))
	for _, id := range rebalanceInProgress {
		inProgressSet[id] = struct{}{}
	}
	for _, id := range currentShardAllocations[maxRegion] {
		if _, moving := inProgressSet[id]; !moving {
			return []ShardId{id}
		}
	}
	return nil
}

// LeastShardAllocationStrategy allocates shards to the region with the fewest shards.
type LeastShardAllocationStrategy struct {
	rebalanceThreshold       int
	maxSimultaneousRebalance int
}

func NewLeastShardAllocationStrategy(threshold, maxSimultaneous int) *LeastShardAllocationStrategy {
	return &LeastShardAllocationStrategy{
		rebalanceThreshold:       threshold,
		maxSimultaneousRebalance: maxSimultaneous,
	}
}

func (s *LeastShardAllocationStrategy) AllocateShard(requester actor.Ref, shardId ShardId, currentShardAllocations map[actor.Ref][]ShardId) actor.Ref {
	var minRegion actor.Ref
	minCount := -1

	for region, shards := range currentShardAllocations {
		count := len(shards)
		if minCount == -1 || count < minCount {
			minCount = count
			minRegion = region
		}
	}

	return minRegion
}

func (s *LeastShardAllocationStrategy) Rebalance(currentShardAllocations map[actor.Ref][]ShardId, rebalanceInProgress []ShardId) []ShardId {
	if len(rebalanceInProgress) >= s.maxSimultaneousRebalance {
		return nil
	}

	var minCount = -1
	var maxCount = -1
	var maxRegion actor.Ref

	for _, shards := range currentShardAllocations {
		count := len(shards)
		if minCount == -1 || count < minCount {
			minCount = count
		}
		if maxCount == -1 || count > maxCount {
			maxCount = count
		}
	}

	if maxCount-minCount <= s.rebalanceThreshold {
		return nil
	}

	for region, shards := range currentShardAllocations {
		if len(shards) == maxCount {
			maxRegion = region
			break
		}
	}

	if maxRegion == nil {
		return nil
	}

	// Pick one shard from the most loaded region that is not already being rebalanced
	shards := currentShardAllocations[maxRegion]
	for _, id := range shards {
		inProgress := false
		for _, rip := range rebalanceInProgress {
			if id == rip {
				inProgress = true
				break
			}
		}
		if !inProgress {
			return []ShardId{id}
		}
	}

	return nil
}

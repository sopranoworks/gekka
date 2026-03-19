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

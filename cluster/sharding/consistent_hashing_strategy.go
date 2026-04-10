/*
 * consistent_hashing_strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/sopranoworks/gekka/actor"
)

// ConsistentHashingAllocationStrategy allocates shards to regions using a
// consistent hash ring.  When regions are added or removed only a minimal
// fraction of shards need to be rebalanced (~1/N where N is the number of
// regions).
type ConsistentHashingAllocationStrategy struct {
	virtualNodes int
}

// NewConsistentHashingAllocationStrategy creates a new strategy.  virtualNodes
// controls the number of virtual nodes per region on the hash ring; higher
// values give a more uniform distribution.  If virtualNodes <= 0 it defaults
// to 100.
func NewConsistentHashingAllocationStrategy(virtualNodes int) *ConsistentHashingAllocationStrategy {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	return &ConsistentHashingAllocationStrategy{virtualNodes: virtualNodes}
}

// ringEntry is a single point on the consistent hash ring.
type ringEntry struct {
	hash   uint32
	region actor.Ref
}

// buildRing constructs a sorted slice of ring entries from the current region
// set.  Each region contributes virtualNodes entries whose hash is computed
// from "regionPath#i".
func (s *ConsistentHashingAllocationStrategy) buildRing(regions map[actor.Ref][]ShardId) []ringEntry {
	ring := make([]ringEntry, 0, len(regions)*s.virtualNodes)
	for region := range regions {
		path := region.Path()
		for i := 0; i < s.virtualNodes; i++ {
			key := fmt.Sprintf("%s#%d", path, i)
			h := crc32.ChecksumIEEE([]byte(key))
			ring = append(ring, ringEntry{hash: h, region: region})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hash < ring[j].hash
	})
	return ring
}

// lookup finds the region responsible for the given shardId on the hash ring
// using binary search with wrap-around.
func (s *ConsistentHashingAllocationStrategy) lookup(ring []ringEntry, shardId ShardId) actor.Ref {
	if len(ring) == 0 {
		return nil
	}
	h := crc32.ChecksumIEEE([]byte(shardId))
	idx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= h
	})
	if idx == len(ring) {
		idx = 0 // wrap around
	}
	return ring[idx].region
}

// AllocateShard returns the region that should host the given shard according
// to the consistent hash ring built from the current allocations.
func (s *ConsistentHashingAllocationStrategy) AllocateShard(requester actor.Ref, shardId ShardId, currentShardAllocations map[actor.Ref][]ShardId) actor.Ref {
	ring := s.buildRing(currentShardAllocations)
	return s.lookup(ring, shardId)
}

// Rebalance returns at most one shard that is currently assigned to a region
// different from where the consistent hash ring says it should be.  It skips
// rebalancing entirely when there is already a rebalance in progress.
func (s *ConsistentHashingAllocationStrategy) Rebalance(currentShardAllocations map[actor.Ref][]ShardId, rebalanceInProgress []ShardId) []ShardId {
	if len(rebalanceInProgress) > 0 {
		return nil
	}

	ring := s.buildRing(currentShardAllocations)
	if len(ring) == 0 {
		return nil
	}

	for region, shards := range currentShardAllocations {
		for _, shardId := range shards {
			ideal := s.lookup(ring, shardId)
			if ideal != nil && ideal.Path() != region.Path() {
				return []ShardId{shardId}
			}
		}
	}
	return nil
}

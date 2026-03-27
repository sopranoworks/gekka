/*
 * adaptive_strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// ── MetricsReader ─────────────────────────────────────────────────────────────

// MetricsReader supplies per-node load metrics to AdaptiveAllocationStrategy.
type MetricsReader interface {
	// NodeLoad returns a combined load score in [0.0, 1.0] for the region
	// identified by regionPath. Lower is better.
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

// GossipMetricsReader implements MetricsReader by querying the real-time
// metrics collected by cluster.MetricsGossip.
type GossipMetricsReader struct {
	mg            *cluster.MetricsGossip
	CPUWeight     float64
	MemoryWeight  float64
	MailboxWeight float64
}

// NewGossipMetricsReader creates a new GossipMetricsReader with configurable weights.
func NewGossipMetricsReader(mg *cluster.MetricsGossip, cpu, mem, mailbox float64) *GossipMetricsReader {
	// Normalize weights so they sum to 1.0
	total := cpu + mem + mailbox
	if total > 0 {
		cpu /= total
		mem /= total
		mailbox /= total
	} else {
		// Default to original MetricsCollector weights if not provided
		cpu, mem, mailbox = 0.4, 0.3, 0.3
	}

	return &GossipMetricsReader{
		mg:            mg,
		CPUWeight:     cpu,
		MemoryWeight:  mem,
		MailboxWeight: mailbox,
	}
}

func (r *GossipMetricsReader) NodeLoad(regionPath string) float64 {
	path, err := actor.ParseActorPath(regionPath)
	if err != nil {
		return 0.5
	}
	nodeID := fmt.Sprintf("%s:%d", path.Address.Host, path.Address.Port)
	pressures := r.mg.ClusterPressure()
	p, ok := pressures[nodeID]
	if !ok {
		fmt.Printf("GossipMetricsReader: node %s not found in gossip (total=%d)\n", nodeID, len(pressures))
		return 0.5
	}

	// Normalize Memory (assume 1GB is 'full' for scoring purposes)
	const maxHeap = 1024 * 1024 * 1024
	heapNorm := float64(p.HeapMemory) / maxHeap
	if heapNorm > 1.0 {
		heapNorm = 1.0
	}

	// Normalize Mailbox (assume 10000 total pending messages is 'full')
	const maxMailbox = 10000
	mailboxNorm := float64(p.MailboxSize) / maxMailbox
	if mailboxNorm > 1.0 {
		mailboxNorm = 1.0
	}

	score := (p.CPUUsage * r.CPUWeight) + (heapNorm * r.MemoryWeight) + (mailboxNorm * r.MailboxWeight)
	fmt.Printf("GossipMetricsReader: node %s score=%f (cpu=%f, mem=%f, mailbox=%d)\n", nodeID, score, p.CPUUsage, heapNorm, p.MailboxSize)
	return score
}

// ── AdaptiveAllocationStrategy ────────────────────────────────────────────────

// AdaptiveAllocationStrategy allocates shards to the region with the lowest
// combined score:
//
//	score = loadWeight * nodeLoad(region) + (1-loadWeight) * normalised_shard_count
type AdaptiveAllocationStrategy struct {
	metrics                  MetricsReader
	loadWeight               float64 // [0.0, 1.0]; 0 = pure shard-count, 1 = pure load
	rebalanceThreshold       float64 // minimum score spread that triggers rebalance
	maxSimultaneousRebalance int
}

// NewAdaptiveAllocationStrategy creates an AdaptiveAllocationStrategy.
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

func (s *AdaptiveAllocationStrategy) score(ref actor.Ref, shardCount, totalShards int) float64 {
	load := s.metrics.NodeLoad(ref.Path())

	var shardFraction float64
	if totalShards > 0 {
		shardFraction = float64(shardCount) / float64(totalShards)
	}

	return s.loadWeight*load + (1-s.loadWeight)*shardFraction
}

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

	inProgressSet := make(map[ShardId]struct{}, len(rebalanceInProgress))
	for _, id := range rebalanceInProgress {
		inProgressSet[id] = struct{}{}
	}

	var toMove []ShardId
	maxToMove := s.maxSimultaneousRebalance - len(rebalanceInProgress)

	for _, id := range currentShardAllocations[maxRegion] {
		if _, moving := inProgressSet[id]; !moving {
			toMove = append(toMove, id)
			if len(toMove) >= maxToMove {
				break
			}
		}
	}

	return toMove
}

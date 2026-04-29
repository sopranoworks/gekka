/*
 * strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"sort"
	"time"

	"github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
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

// LeastShardAllocationStrategyV2 implements the Pekko 1.0+ two-phase
// least-shard-allocation algorithm. It is selected by
// `pekko.cluster.sharding.least-shard-allocation-strategy.rebalance-absolute-limit > 0`.
//
// Phase 1: every region with more shards than the optimal `ceil(total/regions)`
// contributes its excess; the result is capped by the per-round limit derived
// from absoluteLimit and relativeLimit.
//
// Phase 2: only when phase 1 returns nothing. If any region is at least 2 below
// optimal, take the head shard of every region that is at or above optimal
// (capped by `min(countBelowOptimal, limit)`).
type LeastShardAllocationStrategyV2 struct {
	absoluteLimit int
	relativeLimit float64
}

func NewLeastShardAllocationStrategyV2(absoluteLimit int, relativeLimit float64) *LeastShardAllocationStrategyV2 {
	return &LeastShardAllocationStrategyV2{
		absoluteLimit: absoluteLimit,
		relativeLimit: relativeLimit,
	}
}

func (s *LeastShardAllocationStrategyV2) AllocateShard(requester actor.Ref, shardId ShardId, currentShardAllocations map[actor.Ref][]ShardId) actor.Ref {
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

// limit returns the per-round shard cap as max(1, min(relative*total, absolute)).
// Mirrors Pekko's LeastShardAllocationStrategy.limit; never returns < 1 when the
// strategy is active (selection at the call site already requires absolute > 0).
func (s *LeastShardAllocationStrategyV2) limit(total int) int {
	rel := int(s.relativeLimit * float64(total))
	if rel > s.absoluteLimit {
		rel = s.absoluteLimit
	}
	if rel < 1 {
		rel = 1
	}
	return rel
}

func (s *LeastShardAllocationStrategyV2) Rebalance(currentShardAllocations map[actor.Ref][]ShardId, rebalanceInProgress []ShardId) []ShardId {
	if len(rebalanceInProgress) > 0 {
		// One rebalance round at a time, matching Pekko semantics.
		return nil
	}

	// Snapshot regions in a stable order: descending shard count, then by path
	// so the result is deterministic for a given input.
	type regionEntry struct {
		region actor.Ref
		shards []ShardId
	}
	entries := make([]regionEntry, 0, len(currentShardAllocations))
	total := 0
	for r, sh := range currentShardAllocations {
		entries = append(entries, regionEntry{region: r, shards: sh})
		total += len(sh)
	}
	if total == 0 || len(entries) == 0 {
		return nil
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if len(entries[i].shards) != len(entries[j].shards) {
			return len(entries[i].shards) > len(entries[j].shards)
		}
		return entries[i].region.Path() < entries[j].region.Path()
	})

	// optimalPerRegion = ceil(total / regions).
	regions := len(entries)
	optimal := total / regions
	if total%regions != 0 {
		optimal++
	}

	cap := s.limit(total)

	// Phase 1: trim every over-loaded region down to the optimal.
	var phase1 []ShardId
	for _, e := range entries {
		if len(e.shards) > optimal {
			excess := e.shards[:len(e.shards)-optimal]
			phase1 = append(phase1, excess...)
		}
	}
	if len(phase1) > 0 {
		if len(phase1) > cap {
			phase1 = phase1[:cap]
		}
		return phase1
	}

	// Phase 2: if any region sits ≥ 2 below optimal, donate one head shard from
	// each region currently at or above optimal.
	countBelow := 0
	for _, e := range entries {
		if len(e.shards) < optimal-1 {
			countBelow += (optimal - 1) - len(e.shards)
		}
	}
	if countBelow == 0 {
		return nil
	}
	var phase2 []ShardId
	for _, e := range entries {
		if len(e.shards) >= optimal {
			phase2 = append(phase2, e.shards[0])
		}
	}
	take := countBelow
	if cap < take {
		take = cap
	}
	if len(phase2) > take {
		phase2 = phase2[:take]
	}
	return phase2
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

// LoadAllocationStrategy reads the given configuration and instantiates the required shard allocation logic.
func LoadAllocationStrategy(conf config.Config, cm *cluster.ClusterManager, fallback ShardAllocationStrategy) ShardAllocationStrategy {
	cfg, err := conf.GetConfig("gekka.cluster.sharding.allocation-strategy")
	if err != nil {
		return fallback
	}

	strategyType, err := cfg.GetString("type")
	if err != nil || strategyType == "" {
		strategyType = "least-shard"
	}

	switch strategyType {
	case "external":
		extCfg, err := cfg.GetConfig("external")
		if err != nil {
			return fallback
		}
		url, _ := extCfg.GetString("url")
		durStr, err := extCfg.GetString("timeout")
		timeout := 5 * time.Second
		if err == nil && durStr != "" {
			if d, parseErr := time.ParseDuration(durStr); parseErr == nil {
				timeout = d
			}
		}
		return NewExternalShardAllocationStrategy(url, timeout, fallback)

	case "dsl":
		dslCfg, err := cfg.GetConfig("dsl")
		if err != nil {
			return fallback
		}
		rule, _ := dslCfg.GetString("rule")
		return NewDSLShardAllocationStrategy(rule, cm, fallback)

	case "consistent-hashing":
		vnodes := 100
		if chCfg, err := cfg.GetConfig("consistent-hashing"); err == nil {
			if v, err := chCfg.GetInt("virtual-nodes-factor"); err == nil && v > 0 {
				vnodes = v
			}
		}
		return NewConsistentHashingAllocationStrategy(vnodes)

	case "least-shard":
		fallthrough
	default:
		return fallback
	}
}

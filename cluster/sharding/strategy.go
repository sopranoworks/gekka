/*
 * strategy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
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

	case "least-shard":
		fallthrough
	default:
		return fallback
	}
}

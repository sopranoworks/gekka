//go:build integration

/*
 * sharding_rebalance_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package integration_test

import (
	"fmt"
	"testing"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/stretchr/testify/assert"
)

// mockBehavior is a simple event-sourced behavior for testing.
func mockBehavior(id string) *ptyped.EventSourcedBehavior[string, string, string] {
	return &ptyped.EventSourcedBehavior[string, string, string]{
		PersistenceID: "entity-" + id,
		CommandHandler: func(ctx typed.TypedContext[string], state string, cmd string) ptyped.Effect[string, string] {
			return ptyped.Persist[string, string](cmd)
		},
		EventHandler: func(state string, event string) string {
			return event
		},
	}
}

func extractEntity(msg any) (sharding.EntityId, sharding.ShardId, any) {
	if env, ok := msg.(sharding.ShardingEnvelope); ok {
		// Predictable shard ID based on entity ID
		shardID := "shard-" + env.EntityId
		return env.EntityId, shardID, env.Message
	}
	if id, ok := msg.(string); ok {
		return id, "shard-" + id, nil
	}
	return "", "", msg
}

func TestAdaptiveShardingRebalance(t *testing.T) {
	// Start node 1
	port1 := freePort(t)
	hocon1 := fmt.Sprintf(`
		pekko.remote.artery.canonical.hostname = "127.0.0.1"
		pekko.remote.artery.canonical.port = %d
		pekko.cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:%d"]
		gekka.cluster.sharding.adaptive-rebalancing {
			enabled = on
			load-weight = 1.0
			rebalance-threshold = 0.1
			max-simultaneous-rebalance = 5
		}
		gekka.cluster.failure-detector.threshold = 20.0
		gekka.cluster.distributed-data {
			enabled = on
			gossip-interval = 200ms
		}
	`, port1, port1)

	cfg1, _ := gekka.ParseHOCONString(hocon1)
	node1, err := gekka.NewCluster(cfg1)
	assert.NoError(t, err)
	defer node1.Shutdown()
	node1.JoinSeeds()

	// Wait for node 1 to be Up and Oldest
	time.Sleep(5 * time.Second)

	// Start sharding on node 1
	settings := gekka.ShardingSettings{
		NumberOfShards: 10,
	}
	_, err = gekka.StartSharding(node1, "TestEntity", mockBehavior, extractEntity, settings)
	assert.NoError(t, err)

	// Allocate some shards on node1
	for i := 0; i < 5; i++ {
		entityID := fmt.Sprintf("e%d", i)
		ref, _ := gekka.EntityRefFor[string](node1, "TestEntity", entityID)
		ref.Tell("hello")
	}

	// Wait for allocation
	time.Sleep(2 * time.Second)

	// Check distribution - should be all on node 1
	var coord *sharding.ShardCoordinator
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		coord, ok = sharding.LookupCoordinator("TestEntity")
		if ok && coord != nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.NotNil(t, coord, "Coordinator should be registered")

	snap := coord.AllocationSnapshot()
	t.Logf("Snapshot before Node 2: %v", snap)
	for _, regionPath := range snap {
		assert.Contains(t, regionPath, fmt.Sprintf(":%d", port1))
	}

	// Start node 2
	port2 := freePort(t)
	hocon2 := fmt.Sprintf(`
		pekko.remote.artery.canonical.hostname = "127.0.0.1"
		pekko.remote.artery.canonical.port = %d
		pekko.cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:%d"]
		gekka.cluster.sharding.adaptive-rebalancing {
			enabled = on
			load-weight = 1.0
			rebalance-threshold = 0.1
			max-simultaneous-rebalance = 5
		}
		gekka.cluster.failure-detector.threshold = 20.0
		gekka.cluster.distributed-data {
			enabled = on
			gossip-interval = 200ms
		}
	`, port2, port1)

	cfg2, _ := gekka.ParseHOCONString(hocon2)
	node2, err := gekka.NewCluster(cfg2)
	assert.NoError(t, err)
	defer node2.Shutdown()
	node2.JoinSeeds()

	// Wait for node 2 to be Up
	time.Sleep(2 * time.Second)

	// Start sharding on node 2
	_, err = gekka.StartSharding(node2, "TestEntity", mockBehavior, extractEntity, settings)
	assert.NoError(t, err)

	// Wait for coordinator to see Node 2
	time.Sleep(1 * time.Second)

	// Simulate high load on node1 via mock
	node1.MetricsGossip().Collector().SetMockPressure(&actor.NodePressure{
		CPUUsage: 0.9,
		Score:    0.9,
	})
	node2.MetricsGossip().Collector().SetMockPressure(&actor.NodePressure{
		CPUUsage: 0.1,
		Score:    0.1,
	})

	// Wait for metrics to propagate and rebalance to trigger
	t.Log("Waiting for rebalance...")
	
	// Speed up rebalance tick for test
	coord.RebalanceInterval = 1 * time.Second

	deadline = time.Now().Add(30 * time.Second)
	rebalanced := false
	for time.Now().Before(deadline) {
		coord.Self().Tell(sharding.RebalanceTick{})

		// We must send messages to entities to trigger GetShardHome,
		// as Rebalance clears the shard allocation but doesn't proactively
		// start shards unless RememberEntities is true.
		for i := 0; i < 5; i++ {
			entityID := fmt.Sprintf("e%d", i)
			ref, _ := gekka.EntityRefFor[string](node1, "TestEntity", entityID)
			ref.Tell("hello")
		}

		snap = coord.AllocationSnapshot()
		// Check if any shard moved to node2
		for _, regionPath := range snap {
			if contains(regionPath, fmt.Sprintf(":%d", port2)) {
				rebalanced = true
				break
			}
		}
		if rebalanced {
			break
		}
		time.Sleep(1 * time.Second)
	}

	t.Logf("Final snapshot: %v", snap)
	assert.True(t, rebalanced, "Shards should have migrated to node2 under pressure on node1")
}

func contains(s, substr string) bool {
	// Simple string contains
	return len(s) >= len(substr) && func() bool {
		for i := 0; i+len(substr) <= len(s); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	}()
}


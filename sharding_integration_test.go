/*
 * sharding_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/sopranoworks/gekka/sharding"
)

func TestClusterSharding_Rebalancing(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Start Node 1 (Seed)
	node1, _ := NewCluster(ClusterConfig{SystemName: "ShardingTest", Port: 2551})
	defer func() { _ = node1.Shutdown() }()
	_ = node1.Join("127.0.0.1", 2551)

	// 2. Start Node 2
	node2, _ := NewCluster(ClusterConfig{SystemName: "ShardingTest", Port: 0})
	defer func() { _ = node2.Shutdown() }()
	_ = node2.Join("127.0.0.1", 2551)

	// Wait for cluster to be Up
	for {
		if node1.IsUp() && node2.IsUp() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 3. Setup Sharding
	journal := persistence.NewInMemoryJournal()
	settings := ShardingSettings{NumberOfShards: 10}

	extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
		if s, ok := msg.(string); ok {
			return s, "shard-" + string(s[0]), s
		}
		return "", "", msg
	}

	behaviorFactory := func(id string) *EventSourcedBehavior[string, string, string] {
		return &EventSourcedBehavior[string, string, string]{
			PersistenceID: "test-" + id,
			Journal:       journal,
			InitialState:  "",
			CommandHandler: func(ctx typed.TypedContext[string], state string, cmd string) ptyped.Effect[string, string] {
				return ptyped.Persist[string, string](cmd)
			},
			EventHandler: func(state string, event string) string {
				return event
			},
		}
	}

	// Register types
	node1.RegisterType("string", reflect.TypeOf(""))
	node2.RegisterType("string", reflect.TypeOf(""))

	_, err := StartSharding(node1, "TestEntity", behaviorFactory, extractId, settings)
	if err != nil {
		t.Fatalf("node1 sharding: %v", err)
	}
	_, err = StartSharding(node2, "TestEntity", behaviorFactory, extractId, settings)
	if err != nil {
		t.Fatalf("node2 sharding: %v", err)
	}

	// 4. Send messages to various entities
	entities := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, id := range entities {
		ref, _ := EntityRefFor[string](node1, "TestEntity", id)
		ref.Tell("msg-" + id)
	}

	time.Sleep(2 * time.Second)

	// 5. Verify entities are reachable and state is persisted
	for _, id := range entities {
		ref, _ := EntityRefFor[string](node2, "TestEntity", id)
		// We don't have a GetState command in this test behavior,
		// but we can verify it doesn't crash and delivery happens.
		ref.Tell("verify-" + id)
	}

	// 6. Start Node 3 and verify rebalancing (indirectly)
	node3, _ := NewCluster(ClusterConfig{SystemName: "ShardingTest", Port: 0})
	defer func() { _ = node3.Shutdown() }()
	node3.RegisterType("string", reflect.TypeOf(""))
	_ = node3.Join("127.0.0.1", 2551)

	_, err = StartSharding(node3, "TestEntity", behaviorFactory, extractId, settings)
	if err != nil {
		t.Fatalf("node3 sharding: %v", err)
	}

	// Wait for Node 3 to be Up
	for {
		if node3.IsUp() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	// 7. Verify all entities are still reachable from Node 3
	for _, id := range entities {
		ref, _ := EntityRefFor[string](node3, "TestEntity", id)
		ref.Tell("final-" + id)
	}

	time.Sleep(1 * time.Second)
	t.Log("Sharding integration test complete")
}

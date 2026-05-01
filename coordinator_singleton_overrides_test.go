/*
 * coordinator_singleton_overrides_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
	"github.com/sopranoworks/gekka/cluster/singleton"
	"github.com/sopranoworks/gekka/persistence"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
)

// Sub-plan 8c: end-to-end verification that the three sharding-coordinator
// singleton overrides parsed from HOCON
// (`pekko.cluster.sharding.coordinator-singleton.{singleton-name,
// hand-over-retry-interval, min-number-of-hand-over-retries}`) are threaded
// onto the ClusterSingletonManager that wraps the ShardCoordinator. Prior
// to 8c they were parsed onto Sharding.CoordinatorSingleton.* but never
// applied — the top-level `StartSharding` spawned the coordinator as a
// plain actor, not under a SingletonManager.
//
// This test boots a single-node *Cluster, calls StartSharding, then
// resolves the manager actor at /user/<typeName>Coordinator and asserts
// the manager's exported accessors return the configured values. The
// manager's accessors expose private fields written by the WithXxx
// setters that are themselves consumed by the manager's lease-acquire
// retry path (manager.go:226–258), so observing the accessors confirms
// the runtime state — not just the parse.
func TestSharding_CoordinatorSingletonOverrides_AppliedToManager(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    sharding {
      coordinator-singleton {
        singleton-name                   = "shardCoord"
        hand-over-retry-interval         = 333ms
        min-number-of-hand-over-retries  = 9
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestCoordSingleton"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	// Minimal behavior factory + extractor so StartSharding can build the
	// manager. The entities are never sent any messages in this test —
	// only the manager's configured state is asserted.
	journal := persistence.NewInMemoryJournal()
	settings := ShardingSettings{NumberOfShards: 4}
	extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
		if s, ok := msg.(string); ok {
			return s, "shard-0", s
		}
		return "", "", msg
	}
	behaviorFactory := func(id string) *ptyped.EventSourcedBehavior[string, string, string] {
		return &ptyped.EventSourcedBehavior[string, string, string]{
			PersistenceID: "test-" + id,
			Journal:       journal,
			InitialState:  "",
			CommandHandler: func(ctx typed.TypedContext[string], state string, cmd string) ptyped.Effect[string, string] {
				return ptyped.Persist[string, string](cmd)
			},
			EventHandler: func(state string, event string) string { return event },
		}
	}
	node.RegisterType("string", reflect.TypeOf(""))

	const typeName = "TestEntity"
	if _, err := StartSharding(node, typeName, behaviorFactory, extractId, settings); err != nil {
		t.Fatalf("StartSharding: %v", err)
	}

	// The manager actor is spawned synchronously inside StartSharding;
	// resolve it at /user/<typeName>Coordinator.
	mgrPath := "/user/" + typeName + "Coordinator"
	a, ok := node.GetLocalActor(mgrPath)
	if !ok {
		t.Fatalf("manager actor not registered at %q", mgrPath)
	}
	mgr, ok := a.(*singleton.ClusterSingletonManager)
	if !ok {
		t.Fatalf("actor at %q is %T, want *singleton.ClusterSingletonManager", mgrPath, a)
	}

	if got, want := mgr.SingletonName(), "shardCoord"; got != want {
		t.Errorf("mgr.SingletonName() = %q, want %q", got, want)
	}
	if got, want := mgr.HandOverRetryInterval(), 333*time.Millisecond; got != want {
		t.Errorf("mgr.HandOverRetryInterval() = %v, want %v", got, want)
	}
	if got, want := mgr.MinHandOverRetries(), 9; got != want {
		t.Errorf("mgr.MinHandOverRetries() = %d, want %d", got, want)
	}
}

// TestSharding_CoordinatorSingletonOverrides_DefaultsPreserved verifies
// that omitting the sharding overrides leaves the manager at its
// built-in defaults (singleton-name="singleton",
// hand-over-retry-interval=1s, min-number-of-hand-over-retries=15) —
// the WithXxx setters are no-ops for zero/empty values.
func TestSharding_CoordinatorSingletonOverrides_DefaultsPreserved(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster { seed-nodes = [] }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestCoordSingletonDefaults"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	journal := persistence.NewInMemoryJournal()
	settings := ShardingSettings{NumberOfShards: 4}
	extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
		if s, ok := msg.(string); ok {
			return s, "shard-0", s
		}
		return "", "", msg
	}
	behaviorFactory := func(id string) *ptyped.EventSourcedBehavior[string, string, string] {
		return &ptyped.EventSourcedBehavior[string, string, string]{
			PersistenceID: "test-" + id,
			Journal:       journal,
			InitialState:  "",
			CommandHandler: func(ctx typed.TypedContext[string], state string, cmd string) ptyped.Effect[string, string] {
				return ptyped.Persist[string, string](cmd)
			},
			EventHandler: func(state string, event string) string { return event },
		}
	}
	node.RegisterType("string", reflect.TypeOf(""))

	const typeName = "TestEntityDefault"
	if _, err := StartSharding(node, typeName, behaviorFactory, extractId, settings); err != nil {
		t.Fatalf("StartSharding: %v", err)
	}

	mgrPath := "/user/" + typeName + "Coordinator"
	a, ok := node.GetLocalActor(mgrPath)
	if !ok {
		t.Fatalf("manager actor not registered at %q", mgrPath)
	}
	mgr, ok := a.(*singleton.ClusterSingletonManager)
	if !ok {
		t.Fatalf("actor at %q is %T, want *singleton.ClusterSingletonManager", mgrPath, a)
	}

	if got, want := mgr.SingletonName(), "singleton"; got != want {
		t.Errorf("mgr.SingletonName() = %q, want %q (default)", got, want)
	}
	if got, want := mgr.HandOverRetryInterval(), 1*time.Second; got != want {
		t.Errorf("mgr.HandOverRetryInterval() = %v, want %v (default)", got, want)
	}
	if got, want := mgr.MinHandOverRetries(), 15; got != want {
		t.Errorf("mgr.MinHandOverRetries() = %d, want %d (default)", got, want)
	}
}

/*
 * cluster_sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"

	"github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	cpersistence "github.com/sopranoworks/gekka/cluster/persistence"
	"github.com/sopranoworks/gekka/cluster/singleton"
)

// ClusterShardingConfig holds all options needed to start cluster sharding for
// one entity type.
type ClusterShardingConfig struct {
	// TypeName is the unique identifier for this entity type (e.g. "Cart", "Order").
	// It is used to derive actor names so that multiple entity types can coexist
	// in the same actor system without name collisions.
	TypeName string

	// EntityProps describes how to create an entity actor given its EntityId.
	EntityProps EntityProps

	// Settings controls passivation, remember-entities, and handoff behaviour.
	Settings ShardSettings

	// Extractor maps every incoming message to an (EntityId, ShardId, payload)
	// triple.  Must be deterministic: the same EntityId must always produce the
	// same ShardId.
	Extractor ExtractEntityId

	// Strategy decides which ShardRegion hosts a newly-allocated shard.
	// When nil, LeastShardAllocationStrategy(rebalanceThreshold=1,
	// maxSimultaneousRebalance=1) is used.
	Strategy ShardAllocationStrategy

	// Role, when non-empty, restricts the coordinator singleton to nodes that
	// carry this cluster role.  Pass "" to allow any node.
	Role string

	// PersistentEntityFactory, when non-nil, is called instead of EntityProps
	// to create each entity actor.  The returned PersistentActor is wrapped in
	// a PersistentActorWrapper so that events are journalled automatically.
	// EntityJournal must also be set when this field is non-nil.
	PersistentEntityFactory func(entityId EntityId) cpersistence.PersistentActor

	// EntityJournal is the Journal used to persist events for sharded entities
	// when PersistentEntityFactory is set.
	EntityJournal cpersistence.Journal

	// EntitySnapshotStore, when non-nil, enables snapshot support for sharded
	// persistent entities.  Optional even when PersistentEntityFactory is set.
	EntitySnapshotStore cpersistence.SnapshotStore
}

// entityIdAdapter wraps a PersistentActor and overrides PersistenceId to
// return the sharding EntityId, so that users do not need to thread the ID
// through their own actor implementations.
type entityIdAdapter struct {
	cpersistence.PersistentActor
	entityId EntityId
}

func (a *entityIdAdapter) PersistenceId() string { return a.entityId }

// StartSharding wires the full cluster-sharding stack and returns the local
// ShardRegion actor's Ref.
//
// It performs three steps in order:
//
//  1. Registers a ShardCoordinator as a ClusterSingleton (via
//     ClusterSingletonManager) at "/user/shardCoordinator-<TypeName>".
//     The singleton runs only on the oldest eligible node; leadership
//     transfers automatically when that node leaves.
//
//  2. Spawns a ShardCoordinatorProxy at
//     "/user/shardCoordinatorProxy-<TypeName>".  The proxy stashes
//     GetShardHome requests while the coordinator singleton is unavailable
//     and retries them once it becomes reachable.
//
//  3. Spawns a ShardRegion at "/user/shardRegion-<TypeName>".  The region
//     accepts application messages, extracts shard IDs via cfg.Extractor,
//     and routes entity envelopes either to a local Shard or to the
//     appropriate remote ShardRegion.
//
// Callers should retain the returned Ref to send messages to local entities.
// On every node in the cluster, call StartSharding with the same TypeName to
// ensure all nodes can host shards and forward messages to each other.
func StartSharding(
	sys actor.ActorContext,
	cm *cluster.ClusterManager,
	router cluster.Router,
	cfg ClusterShardingConfig,
) (actor.Ref, error) {
	strategy := cfg.Strategy
	if strategy == nil {
		fallback := NewLeastShardAllocationStrategy(1, 1)
		type configProvider interface {
			Config() config.Config
		}
		if cp, ok := sys.(configProvider); ok {
			strategy = LoadAllocationStrategy(cp.Config(), cm, fallback)
		} else {
			strategy = fallback
		}
	}

	// ── Step 1: ShardCoordinator as ClusterSingleton ─────────────────────
	//
	// The coordinator manages the authoritative shard→region allocation map.
	// Wrapping it in ClusterSingletonManager ensures exactly one instance is
	// alive in the cluster at any time (always on the oldest eligible node).
	typeName := cfg.TypeName // capture for closure
	coordProps := actor.Props{
		New: func() actor.Actor {
			c := NewShardCoordinator(strategy)
			RegisterCoordinator(typeName, c)
			return c
		},
	}
	mgrName := "shardCoordinator-" + cfg.TypeName
	mgrProps := actor.Props{
		New: func() actor.Actor {
			return singleton.NewClusterSingletonManager(cm, coordProps, cfg.Role)
		},
	}
	if _, err := sys.ActorOf(mgrProps, mgrName); err != nil {
		return nil, fmt.Errorf("sharding: spawn coordinator singleton manager for %q: %w", cfg.TypeName, err)
	}

	// ── Step 2: ShardCoordinatorProxy ────────────────────────────────────
	//
	// The proxy provides a stable local actor.Ref for the ShardRegion to talk
	// to.  Internally it uses ClusterSingletonProxy to resolve the current
	// coordinator location and retries with exponential back-off if the
	// coordinator is temporarily unreachable (e.g. during leader failover).
	//
	// Note: the singleton child is named "singleton" by ClusterSingletonManager,
	// so the full coordinator path is "/user/<mgrName>/singleton".
	singletonPath := "/user/" + mgrName
	coordProxy := singleton.NewClusterSingletonProxy(cm, router, singletonPath, cfg.Role)
	proxyName := "shardCoordinatorProxy-" + cfg.TypeName
	proxyRef, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor { return NewShardCoordinatorProxy(coordProxy) },
	}, proxyName)
	if err != nil {
		return nil, fmt.Errorf("sharding: spawn coordinator proxy for %q: %w", cfg.TypeName, err)
	}

	// ── Step 3: ShardRegion ───────────────────────────────────────────────
	//
	// The region is the entry point for application messages.  It buffers
	// messages while awaiting ShardHome responses and forwards them once the
	// allocation is known — either to a locally-spawned Shard or to the
	// remote ShardRegion on the owning node.
	var entityCreator func(ctx actor.ActorContext, id EntityId) (actor.Ref, error)
	if cfg.PersistentEntityFactory != nil && cfg.EntityJournal != nil {
		entityCreator = func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return ctx.ActorOf(actor.Props{
				New: func() actor.Actor {
					inner := &entityIdAdapter{
						PersistentActor: cfg.PersistentEntityFactory(id),
						entityId:        id,
					}
					if cfg.EntitySnapshotStore != nil {
						return cpersistence.NewPersistentActorWrapper(inner, cfg.EntityJournal, cfg.EntitySnapshotStore)
					}
					return cpersistence.NewPersistentActorWrapper(inner, cfg.EntityJournal)
				},
			}, id)
		}
	} else {
		entityCreator = func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return ctx.ActorOf(actor.Props{
				New: func() actor.Actor { return cfg.EntityProps.New(id) },
			}, id)
		}
	}
	regionName := "shardRegion-" + cfg.TypeName
	regionRef, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor {
			return NewShardRegion(cfg.TypeName, entityCreator, nil, cfg.Extractor, proxyRef, cfg.Settings)
		},
	}, regionName)
	if err != nil {
		return nil, fmt.Errorf("sharding: spawn shard region for %q: %w", cfg.TypeName, err)
	}

	return regionRef, nil
}

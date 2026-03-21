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

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
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
}

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
		strategy = NewLeastShardAllocationStrategy(1, 1)
	}

	// ── Step 1: ShardCoordinator as ClusterSingleton ─────────────────────
	//
	// The coordinator manages the authoritative shard→region allocation map.
	// Wrapping it in ClusterSingletonManager ensures exactly one instance is
	// alive in the cluster at any time (always on the oldest eligible node).
	coordProps := actor.Props{
		New: func() actor.Actor { return NewShardCoordinator(strategy) },
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
	entityCreator := func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
		return ctx.ActorOf(actor.Props{
			New: func() actor.Actor { return cfg.EntityProps.New(id) },
		}, id)
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

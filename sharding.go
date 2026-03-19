/*
 * sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
	styped "github.com/sopranoworks/gekka/cluster/sharding/typed"
)

// ShardingSettings defines configuration for cluster sharding.
type ShardingSettings struct {
	// Role restricts sharding to nodes with this cluster role.
	Role string

	// NumberOfShards is the total number of shards (immutable after start).
	NumberOfShards int

	// AllocationStrategy decides which node hosts a given shard.
	// Defaults to LeastShardAllocationStrategy(threshold=3, maxSimultaneous=1).
	AllocationStrategy sharding.ShardAllocationStrategy

	// PassivationIdleTimeout, when > 0, automatically stops entities idle
	// longer than this duration.
	//
	// HOCON: pekko.cluster.sharding.passivation.idle-timeout
	PassivationIdleTimeout time.Duration

	// RememberEntities, when true, persists entity lifecycle events so
	// entities are re-spawned after a Shard restart or failover.
	//
	// HOCON: pekko.cluster.sharding.remember-entities = on
	RememberEntities bool

	// Journal is used when RememberEntities is true.  Defaults to an
	// InMemoryJournal if nil (use a durable backend in production).
	Journal persistence.Journal

	// DataCenter restricts shard allocation to nodes in this data center.
	// Leave empty to allow placement on any node.
	//
	// HOCON equivalent:
	//   pekko.cluster.multi-data-center.self-data-center = "us-east"
	DataCenter string

	// HandoffTimeout is the maximum time a ShardRegion waits for the
	// coordinator to acknowledge shard handoff during coordinated shutdown.
	// Larger clusters or heavily loaded coordinators may need a longer value.
	// Defaults to 10 seconds when zero or unset.
	//
	// HOCON: gekka.cluster.sharding.handoff-timeout
	HandoffTimeout time.Duration
}

// StartSharding starts cluster sharding for a given entity type.
// It returns an EntityRef that can be used to send messages to entities.
func StartSharding[Command any, Event any, State any](
	sys ActorSystem,
	typeName string,
	behaviorFactory func(entityId string) *ptyped.EventSourcedBehavior[Command, Event, State],
	extract sharding.ExtractEntityId,
	settings ShardingSettings,
) (sharding.ClusterEntityRef[Command], error) {

	// Register sharding types for serialization
	sys.RegisterType("sharding.RegisterRegion", reflect.TypeOf(sharding.RegisterRegion{}))
	sys.RegisterType("sharding.GetShardHome", reflect.TypeOf(sharding.GetShardHome{}))
	sys.RegisterType("sharding.ShardHome", reflect.TypeOf(sharding.ShardHome{}))
	sys.RegisterType("sharding.ShardingEnvelope", reflect.TypeOf(sharding.ShardingEnvelope{}))

	// 1. Start ShardCoordinator as a singleton
	strategy := settings.AllocationStrategy
	if strategy == nil {
		strategy = sharding.NewLeastShardAllocationStrategy(3, 1)
	}

	coordinatorProps := actor.Props{
		New: func() actor.Actor {
			return sharding.NewShardCoordinator(strategy)
		},
	}

	var coordinatorRef actor.Ref
	cluster, ok := sys.(*Cluster)
	if ok {
		// Spawn coordinator if we are the oldest
		ua := cluster.cm.OldestNode(settings.Role)
		localUA := cluster.cm.GetLocalAddress()

		if ua != nil && localUA != nil && ua.GetAddress().GetHostname() == localUA.GetAddress().GetHostname() &&
			ua.GetAddress().GetPort() == localUA.GetAddress().GetPort() &&
			ua.GetUid() == localUA.GetUid() {
			_, _ = sys.ActorOf(coordinatorProps, typeName+"Coordinator")
		}

		// Always use a proxy to reach the coordinator
		proxy := cluster.SingletonProxy("/user/"+typeName+"Coordinator", settings.Role).WithSingletonName("")
		ref, err := sys.ActorOf(actor.Props{
			New: func() actor.Actor {
				return sharding.NewShardCoordinatorProxy(proxy)
			},
		}, typeName+"CoordinatorProxy")
		if err != nil {
			return sharding.ClusterEntityRef[Command]{}, err
		}
		coordinatorRef = ref
	}

	// 2. Start ShardRegion
	entityCreator := func(ctx actor.ActorContext, entityId sharding.EntityId) (actor.Ref, error) {
		p := actor.Props{
			New: func() actor.Actor {
				return ptyped.NewPersistentActor(behaviorFactory(entityId))
			},
		}
		return ctx.ActorOf(p, entityId)
	}

	unmarshaler := func(manifest string, data json.RawMessage) (any, error) {
		typ, ok := sys.GetTypeByManifest(manifest)
		if !ok {
			return nil, fmt.Errorf("sharding: no type registered for manifest %q", manifest)
		}

		var ptr reflect.Value
		if typ.Kind() == reflect.Ptr {
			ptr = reflect.New(typ.Elem())
		} else {
			ptr = reflect.New(typ)
		}
		if err := json.Unmarshal(data, ptr.Interface()); err != nil {
			return nil, err
		}
		if typ.Kind() == reflect.Ptr {
			return ptr.Interface(), nil
		}
		return ptr.Elem().Interface(), nil
	}

	shardSettings := sharding.ShardSettings{
		PassivationIdleTimeout: settings.PassivationIdleTimeout,
		RememberEntities:       settings.RememberEntities,
		Journal:                settings.Journal,
		DataCenter:             settings.DataCenter,
		HandoffTimeout:         settings.HandoffTimeout,
	}

	// Populate IsLocalDC when both the cluster and DataCenter are available.
	if cluster != nil && settings.DataCenter != "" {
		dc := settings.DataCenter
		shardSettings.IsLocalDC = func(host string, port uint32) bool {
			return cluster.cm.IsInDataCenter(host, port, dc)
		}
	}

	region, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor {
			return sharding.NewShardRegion(typeName, entityCreator, unmarshaler, extract, coordinatorRef, shardSettings)
		},
	}, typeName+"Region")

	if err != nil {
		return sharding.ClusterEntityRef[Command]{}, fmt.Errorf("failed to spawn region: %w", err)
	}

	return sharding.ClusterEntityRef[Command]{
		EntityId: "", // entry point for the region
		Region:   region,
	}, nil
}

// EntityRefFor returns a type-safe EntityRef for a specific entity.
func EntityRefFor[M any](sys ActorSystem, typeName string, entityID string) (*styped.EntityRef[M], error) {
	regionPath := "/user/" + typeName + "Region"
	ref, err := sys.ActorSelection(regionPath).Resolve(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("sharding: failed to resolve region %q: %w", regionPath, err)
	}
	return styped.NewEntityRef[M](typeName, entityID, ref), nil
}

// StartTyped starts cluster sharding for a given typed entity.
// It returns the ShardRegion actor reference.
func StartTyped[M any, Event any, State any](
	sys ActorSystem,
	typeName string,
	behaviorFactory func(entityId string) *ptyped.EventSourcedBehavior[M, Event, State],
	extract sharding.ExtractEntityId,
	settings ShardingSettings,
) (actor.Ref, error) {
	res, err := StartSharding(sys, typeName, behaviorFactory, extract, settings)
	if err != nil {
		return nil, err
	}
	return res.Region, nil
}

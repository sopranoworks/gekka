/*
 * sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"encoding/json"
	"fmt"
	"reflect"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/sharding"
)

// ShardingSettings defines configuration for cluster sharding.
type ShardingSettings struct {
	Role               string
	NumberOfShards     int
	AllocationStrategy sharding.ShardAllocationStrategy
}

// StartSharding starts cluster sharding for a given entity type.
// It returns an EntityRef that can be used to send messages to entities.
func StartSharding[Command any, Event any, State any](
	sys ActorSystem,
	typeName string,
	behaviorFactory func(entityId string) *EventSourcedBehavior[Command, Event, State],
	extract sharding.ExtractEntityId,
	settings ShardingSettings,
) (sharding.EntityRef[Command], error) {

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
			return sharding.EntityRef[Command]{}, err
		}
		coordinatorRef = ref
	}

	// 2. Start ShardRegion
	entityCreator := func(ctx actor.ActorContext, entityId sharding.EntityId) (actor.Ref, error) {
		p := actor.Props{
			New: func() actor.Actor {
				return actor.NewPersistentActor(behaviorFactory(entityId))
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

	region, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor {
			return sharding.NewShardRegion(typeName, entityCreator, unmarshaler, extract, coordinatorRef)
		},
	}, typeName+"Region")

	if err != nil {
		return sharding.EntityRef[Command]{}, fmt.Errorf("failed to spawn region: %w", err)
	}

	return sharding.EntityRef[Command]{
		EntityId: "", // entry point for the region
		Region:   region,
	}, nil
}

// GetEntityRef returns an EntityRef for a specific entity.
func GetEntityRef[T any](sys ActorSystem, typeName string, entityId string) (sharding.EntityRef[T], error) {
	// ShardRegion is registered at typeName+"Region"
	regionPath := "/user/" + typeName + "Region"
	
	// Resolve the region reference. 
	// In a clustered environment, we can use ActorSelection to find it locally.
	ref, err := sys.ActorSelection(regionPath).Resolve(nil)
	if err != nil {
		return sharding.EntityRef[T]{}, fmt.Errorf("sharding: failed to resolve region %q: %w", regionPath, err)
	}
	
	return sharding.EntityRef[T]{
		EntityId: entityId,
		Region:   ref,
	}, nil
}

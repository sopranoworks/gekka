/*
 * sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"encoding/json"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
)

// EntityTypeKey is a type-safe key representing the entity type and its message protocol.
type EntityTypeKey[M any] struct {
	Name string
}

// NewEntityTypeKey creates a new EntityTypeKey for a message type M.
func NewEntityTypeKey[M any](name string) EntityTypeKey[M] {
	return EntityTypeKey[M]{Name: name}
}

// Entity configuration for starting sharded entities.
type Entity[M any] struct {
	TypeKey         EntityTypeKey[M]
	CreateBehavior  func(entityId string) typed.Behavior[M]
	Settings        sharding.ShardSettings
	ExtractEntityId sharding.ExtractEntityId
}

// ClusterSharding is the entry point for typed sharding operations.
type ClusterSharding struct {
	sys         actor.ActorContext
	coordinator actor.Ref
}

// NewClusterSharding creates a new ClusterSharding extension.
func NewClusterSharding(sys actor.ActorContext, coordinator actor.Ref) *ClusterSharding {
	return &ClusterSharding{
		sys:         sys,
		coordinator: coordinator,
	}
}

// Init starts the ShardRegion for an entity type and returns its typed reference.
func (s *ClusterSharding) Init(entity Entity[any]) typed.TypedActorRef[any] {
	unmarshaler := func(manifest string, data json.RawMessage) (any, error) {
		var msg any
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	}

	region, err := s.sys.ActorOf(actor.Props{
		New: func() actor.Actor {
			return sharding.NewShardRegion(entity.TypeKey.Name, func(ctx actor.ActorContext, id string) (actor.Ref, error) {
				behavior := entity.CreateBehavior(id)
				p := actor.Props{
					New: func() actor.Actor { return typed.NewTypedActor(behavior) },
				}
				return ctx.ActorOf(p, id)
			}, unmarshaler, entity.ExtractEntityId, s.coordinator, entity.Settings)
		},
	}, entity.TypeKey.Name+"Region")

	if err != nil {
		// In Pekko, Init usually returns the ref even if it fails to start (e.g. if it's already started).
		// For simplicity, we return a zero ref or handle error if critical.
		return typed.TypedActorRef[any]{}
	}

	return typed.NewTypedActorRef[any](region)
}

// EntityRefFor returns a type-safe reference to a specific entity.
func (s *ClusterSharding) EntityRefFor(typeKey EntityTypeKey[any], entityID string, region actor.Ref) *EntityRef[any] {
	return NewEntityRef[any](typeKey.Name, entityID, region)
}

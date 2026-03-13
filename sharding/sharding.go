/*
 * sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"encoding/json"
	"fmt"
	"reflect"
	"github.com/sopranoworks/gekka/actor"
)

type EntityId = string
type ShardId = string

// ShardingEnvelope is a standard envelope for sharded messages.
type ShardingEnvelope struct {
	EntityId        EntityId
	ShardId         ShardId
	Message         json.RawMessage
	MessageManifest string // manifest of the message for serialization
}

// ExtractEntityId pulls entity ID, shard ID and the actual message from an incoming message.
type ExtractEntityId func(msg any) (EntityId, ShardId, any)

// ShardAllocationStrategy decides where shards should live.
type ShardAllocationStrategy interface {
	// AllocateShard returns the ShardRegion actor reference where the shard should be allocated.
	AllocateShard(requester actor.Ref, shardId ShardId, currentShardAllocations map[actor.Ref][]ShardId) actor.Ref
	
	// Rebalance returns the list of shard IDs that should be rebalanced.
	Rebalance(currentShardAllocations map[actor.Ref][]ShardId, rebalanceInProgress []ShardId) []ShardId
}

// EntityRef[T] is a location-transparent handle to send messages to a specific entity.
type EntityRef[T any] struct {
	EntityId EntityId
	Region   actor.Ref
}

func (r EntityRef[T]) Tell(msg T) {
	if r.EntityId == "" {
		fmt.Printf("EntityRef.Tell (direct): msg=%T\n", msg)
		r.Region.Tell(msg)
		return
	}
	data, _ := json.Marshal(msg)
	fmt.Printf("EntityRef.Tell (wrapped): entityId=%s manifest=%s\n", r.EntityId, reflect.TypeOf(msg).String())
	r.Region.Tell(ShardingEnvelope{
		EntityId:        r.EntityId,
		Message:         data,
		MessageManifest: reflect.TypeOf(msg).String(),
	})
}

// Internal messages for sharding coordinator and regions.
type (
	// RegisterRegion is sent by ShardRegion to ShardCoordinator.
	RegisterRegion struct {
		RegionPath string
	}

	// GetShardHome is sent by ShardRegion to ShardCoordinator to find where a shard lives.
	GetShardHome struct {
		ShardId ShardId
	}

	// ShardHome is the response from ShardCoordinator.
	ShardHome struct {
		ShardId    ShardId
		RegionPath string
	}

	// ShardHomes is sent by ShardCoordinator to all ShardRegions when allocations change.
	ShardHomes struct {
		Homes map[ShardId]string
	}
)

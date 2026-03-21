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

// ClusterEntityRef is a location-transparent handle to send messages to a specific entity.
// Deprecated: use EntityRef[T] for type-safe interaction.
type ClusterEntityRef[T any] struct {
	EntityId EntityId
	Region   actor.Ref
}

func (r ClusterEntityRef[T]) Tell(msg T) {
	if r.EntityId == "" {
		r.Region.Tell(msg)
		return
	}
	data, _ := json.Marshal(msg)
	r.Region.Tell(ShardingEnvelope{
		EntityId:        r.EntityId,
		Message:         data,
		MessageManifest: reflect.TypeOf(msg).String(),
	})
}

// EntityProps defines how sharded entities are created.
type EntityProps struct {
	// New is a factory function that creates a new entity actor instance for a given entity ID.
	New func(entityId EntityId) actor.Actor
}

// StartTyped starts cluster sharding for a given typed entity.
func StartTyped[M any](
	sys actor.ActorContext,
	typeName string,
	entityProps EntityProps,
	settings ShardSettings,
	extract ExtractEntityId,
	coordinator actor.Ref,
) (*ShardRegion, error) {
	// messageUnmarshaler is needed but since we are typed, we can assume JSON for now
	// or use a generic unmarshaler.
	unmarshaler := func(manifest string, data json.RawMessage) (any, error) {
		var msg M
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	}

	region := NewShardRegion(typeName, func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
		p := actor.Props{
			New: func() actor.Actor { return entityProps.New(id) },
		}
		return ctx.ActorOf(p, id)
	}, unmarshaler, extract, coordinator, settings)

	return region, nil
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

	// RegionHandoffRequest is sent by ShardRegion.PostStop to the coordinator
	// during coordinated shutdown to request that all locally-owned shards be
	// released so the coordinator can reallocate them to surviving regions.
	// This must complete before PhaseClusterLeave runs.
	RegionHandoffRequest struct {
		// RegionPath is the Artery actor path of the departing region.
		RegionPath string
	}

	// HandoffComplete is the coordinator's acknowledgement that all shards
	// previously owned by RegionPath have been released from its allocation
	// table.  The departing region uses this as the signal that handoff is
	// done and it is safe to proceed with the cluster Leave.
	HandoffComplete struct {
		// RegionPath echoes the path from the corresponding RegionHandoffRequest.
		RegionPath string
	}

	// ── Shard-level rebalancing protocol ─────────────────────────────────────
	//
	// Rebalancing moves individual shards between regions without shutting down
	// the entire region.  The three-message exchange mirrors Pekko's
	// ClusterSharding rebalancing protocol (serializer manifests BH/BI/BJ/BK).

	// BeginHandOff is sent by ShardCoordinator to the owning ShardRegion to
	// start a shard rebalancing handoff.  The region must stop routing new
	// messages to the shard and reply with BeginHandOffAck.
	BeginHandOff struct {
		ShardId ShardId
	}

	// BeginHandOffAck is sent by ShardRegion to ShardCoordinator to confirm
	// receipt of BeginHandOff.  The coordinator then sends HandOff to instruct
	// the region to drain and stop the shard's entities.
	BeginHandOffAck struct {
		ShardId ShardId
	}

	// HandOff is sent by ShardCoordinator to the owning ShardRegion after it
	// receives BeginHandOffAck.  This is the coordinator's authorisation to
	// stop all entities in the shard.  Once the shard is empty the region
	// replies with ShardStopped.
	HandOff struct {
		ShardId ShardId
	}

	// ShardStopped is sent by ShardRegion to ShardCoordinator once all
	// entities in the handed-off shard have been stopped and the shard is
	// empty.  The coordinator clears the allocation so that the next
	// GetShardHome re-assigns the shard to a less-loaded region.
	ShardStopped struct {
		ShardId ShardId
	}

	// RebalanceShard is a control message that operators or tooling can send
	// directly to a ShardCoordinator to move a specific shard to a named
	// target region, bypassing the automatic allocation strategy.
	//
	// The coordinator initiates the standard BeginHandOff → HandOff →
	// ShardStopped sequence against the current shard owner.  When the shard
	// is cleared it is pre-assigned to TargetRegion so the next GetShardHome
	// for that shard goes to the desired region rather than being re-allocated
	// by the strategy.
	//
	// If ShardId is not currently allocated, or TargetRegion is not a
	// registered region, the message is silently dropped (idempotent).
	RebalanceShard struct {
		ShardId      ShardId
		TargetRegion string // actor path of the destination ShardRegion
	}
)

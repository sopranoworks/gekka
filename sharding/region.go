/*
 * region.go
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
	"time"

	"github.com/sopranoworks/gekka/actor"
)

type ShardRegion struct {
	actor.BaseActor
	typeName           string
	entityCreator      func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error)
	messageUnmarshaler func(manifest string, data json.RawMessage) (any, error)
	extractEntityId    ExtractEntityId
	coordinator        actor.Ref
	shardSettings      ShardSettings

	shards          map[ShardId]actor.Ref // Local shards
	shardHomePaths  map[ShardId]string    // ShardId -> Region Path (cached)
	pendingMessages map[ShardId][]actor.Envelope

	// handoffDone is closed by Receive when a HandoffComplete message arrives
	// from the coordinator.  PostStop blocks on this channel (with a timeout)
	// to ensure the coordinator has released all locally-owned shards before
	// the region actor exits the actor system.
	handoffDone chan struct{}
}

func NewShardRegion(
	typeName string,
	creator func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error),
	unmarshaler func(string, json.RawMessage) (any, error),
	extract ExtractEntityId,
	coordinator actor.Ref,
	shardSettings ShardSettings,
) *ShardRegion {
	return &ShardRegion{
		BaseActor:          actor.NewBaseActor(),
		typeName:           typeName,
		entityCreator:      creator,
		messageUnmarshaler: unmarshaler,
		extractEntityId:    extract,
		coordinator:        coordinator,
		shardSettings:      shardSettings,
		shards:             make(map[ShardId]actor.Ref),
		shardHomePaths:     make(map[ShardId]string),
		pendingMessages:    make(map[ShardId][]actor.Envelope),
		handoffDone:        make(chan struct{}),
	}
}

func (r *ShardRegion) PreStart() {
	// Register with coordinator
	if r.coordinator != nil {
		r.Log().Debug("Registering region with coordinator", "path", r.Self().Path())
		r.coordinator.Tell(RegisterRegion{RegionPath: r.Self().Path()}, r.Self())
	}
}

// PostStop is called by the actor runtime after the mailbox is drained and the
// region actor is about to exit.  It sends RegionHandoffRequest to the
// coordinator and waits up to 10 seconds for HandoffComplete.  This ensures
// the coordinator releases all locally-owned shards — making them available
// for reallocation on surviving nodes — before the coordinated-shutdown
// sequence proceeds to PhaseClusterLeave.
//
// If the coordinator is unreachable (e.g. it runs on the same node and shut
// down first), the wait times out and the region logs a warning rather than
// blocking indefinitely.
func (r *ShardRegion) PostStop() {
	if r.coordinator == nil {
		return
	}
	r.Log().Info("ShardRegion stopping: sending handoff request to coordinator",
		"region", r.Self().Path())
	r.coordinator.Tell(RegionHandoffRequest{RegionPath: r.Self().Path()}, r.Self())

	select {
	case <-r.handoffDone:
		r.Log().Info("Handoff Completed", "region", r.Self().Path())
	case <-time.After(10 * time.Second):
		r.Log().Warn("ShardRegion handoff timed out — proceeding without coordinator ack",
			"region", r.Self().Path())
	}
}

func (r *ShardRegion) Receive(msg any) {
	r.Log().Debug("Region received message", "type", fmt.Sprintf("%T", msg))
	switch m := msg.(type) {
	case ShardingEnvelope:
		r.Log().Debug("Routing ShardingEnvelope", "shardId", m.ShardId, "entityId", m.EntityId)
		r.deliverMessageWithSender(m.ShardId, m, r.Sender())

	case ShardHome:
		r.Log().Debug("Received ShardHome", "shardId", m.ShardId, "region", m.RegionPath)
		r.shardHomePaths[m.ShardId] = m.RegionPath
		// Deliver pending messages
		if msgs, ok := r.pendingMessages[m.ShardId]; ok {
			r.Log().Debug("Delivering pending messages", "shardId", m.ShardId, "count", len(msgs))
			for _, env := range msgs {
				if shardingEnv, ok := env.Payload.(ShardingEnvelope); ok {
					r.deliverMessageWithSender(m.ShardId, shardingEnv, env.Sender)
				}
			}
			delete(r.pendingMessages, m.ShardId)
		}

	case HandoffComplete:
		// Coordinator has released all locally-owned shards; signal PostStop.
		r.Log().Info("Received HandoffComplete from coordinator", "region", m.RegionPath)
		select {
		case <-r.handoffDone:
			// Already closed — ignore duplicate acks.
		default:
			close(r.handoffDone)
		}

	case actor.TerminatedMessage:
		// Handle shard termination or coordinator termination
		terminated := m.TerminatedActor()
		for sid, shard := range r.shards {
			if shard.Path() == terminated.Path() {
				delete(r.shards, sid)
				r.Log().Debug("Shard terminated", "shardId", sid)
				break
			}
		}
		if r.coordinator != nil && r.coordinator.Path() == terminated.Path() {
			r.Log().Error("ShardCoordinator terminated")
		}

	default:
		// Try to extract entity ID from any message
		entityId, shardId, message := r.extractEntityId(msg)
		if entityId != "" {
			data, _ := json.Marshal(message)
			envelope := ShardingEnvelope{
				EntityId:        entityId,
				ShardId:         shardId,
				Message:         data,
				MessageManifest: reflect.TypeOf(message).String(),
			}
			r.deliverMessageWithSender(shardId, envelope, r.Sender())
		}
	}
}


func (r *ShardRegion) deliverMessageWithSender(shardId ShardId, envelope ShardingEnvelope, sender actor.Ref) {
	homePath, ok := r.shardHomePaths[shardId]
	if !ok || homePath == "" {
		// Ask coordinator for home
		r.Log().Debug("Requesting shard home", "shardId", shardId)
		r.pendingMessages[shardId] = append(r.pendingMessages[shardId], actor.Envelope{Payload: envelope, Sender: sender})
		if r.coordinator != nil {
			r.coordinator.Tell(GetShardHome{ShardId: shardId}, r.Self())
		}
		return
	}

	r.Log().Debug("Delivering shard message", "shardId", shardId, "homePath", homePath, "selfPath", r.Self().Path())

	if homePath == r.Self().Path() {
		// Local shard
		shard, ok := r.shards[shardId]
		if !ok {
			// Spawn shard
			r.Log().Debug("Spawning local shard", "shardId", shardId)
			var err error
			sid := shardId // capture for closure
			shard, err = r.System().ActorOf(actor.Props{
				New: func() actor.Actor {
					return NewShard(r.typeName, sid, r.entityCreator, r.messageUnmarshaler, r.shardSettings)
				},
			}, shardId)
			if err != nil {
				r.Log().Error("Failed to spawn shard", "shardId", shardId, "error", err)
				return
			}
			r.shards[shardId] = shard
			r.System().Watch(r.Self(), shard)
		}
		shard.Tell(envelope, sender)
	} else {
		// Remote shard
		r.Log().Debug("Forwarding message to remote region", "shardId", shardId, "region", homePath)
		// Use System().Resolve to get a Ref for the remote path
		remoteRegion, err := r.System().Resolve(homePath)
		if err != nil {
			r.Log().Error("Failed to resolve remote region", "path", homePath, "error", err)
			return
		}
		remoteRegion.Tell(envelope, sender)
	}
}

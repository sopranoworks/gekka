/*
 * region.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/telemetry"
)

type ShardRegion struct {
	actor.BaseActor
	typeName           string
	entityCreator      func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error)
	messageUnmarshaler func(manifest string, data json.RawMessage) (any, error)
	extractEntityId    ExtractEntityId
	coordinator        actor.Ref
	shardSettings      ShardSettings

	shards            map[ShardId]actor.Ref // Local shards
	shardHomePaths    map[ShardId]string    // ShardId -> Region Path (cached)
	pendingMessages   map[ShardId][]actor.Envelope
	handoffInProgress map[ShardId]struct{} // shards being rebalanced away
	drainPending      map[ShardId]struct{} // shards awaiting ShardDrainResponse

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
		handoffInProgress:  make(map[ShardId]struct{}),
		drainPending:       make(map[ShardId]struct{}),
		handoffDone:        make(chan struct{}),
	}
}

func (r *ShardRegion) PreStart() {
	// Register with coordinator
	if r.coordinator != nil {
		r.Log().Debug("Registering region with coordinator", "path", r.Self().Path())
		r.coordinator.Tell(RegisterRegion{RegionPath: r.Self().Path()}, r.Self())
	}
	// Schedule the first retry tick when retry-interval > 0. Each tick re-asks
	// the coordinator for any shard whose home is still unknown (pending). The
	// handler re-arms the ticker so the loop keeps running until the actor
	// stops.
	if r.shardSettings.RetryInterval > 0 {
		r.scheduleRetryTick()
	}
}

// retryShardHomeTickMsg is the self-tick that drives periodic GetShardHome
// retries when the coordinator is briefly unavailable.
type retryShardHomeTickMsg struct{}

// shardFailureBackoffElapsedMsg is delivered to the region after
// ShardSettings.ShardFailureBackoff has elapsed since a Shard actor terminated.
// The handler clears the cached shard-home so the next pending message
// re-resolves via the coordinator.
type shardFailureBackoffElapsedMsg struct{ ShardId ShardId }

// coordinatorFailureBackoffElapsedMsg is delivered to the region after
// ShardSettings.CoordinatorFailureBackoff has elapsed since the
// coordinator actor terminated.
type coordinatorFailureBackoffElapsedMsg struct{}

// scheduleRetryTick fires retryShardHomeTickMsg after RetryInterval; the
// Receive handler re-arms it.
func (r *ShardRegion) scheduleRetryTick() {
	interval := r.shardSettings.RetryInterval
	if interval <= 0 {
		return
	}
	self := r.Self()
	if self == nil {
		return
	}
	time.AfterFunc(interval, func() {
		self.Tell(retryShardHomeTickMsg{})
	})
}

// retryPendingHomes re-asks the coordinator for every shard with buffered
// messages whose home is still unknown.
func (r *ShardRegion) retryPendingHomes() {
	if r.coordinator == nil {
		return
	}
	for sid, queue := range r.pendingMessages {
		if len(queue) == 0 {
			continue
		}
		if home, ok := r.shardHomePaths[sid]; ok && home != "" {
			continue
		}
		r.Log().Debug("retry-interval: re-asking coordinator for shard home",
			"shardId", sid, "queueLen", len(queue))
		r.coordinator.Tell(GetShardHome{ShardId: sid}, r.Self())
	}
}

// PostStop is called by the actor runtime after the mailbox is drained and the
// region actor is about to exit.  It sends RegionHandoffRequest to the
// coordinator and waits for HandoffComplete.  The wait duration is controlled
// by ShardSettings.HandoffTimeout (default 10 s).  This ensures the coordinator
// releases all locally-owned shards — making them available for reallocation on
// surviving nodes — before the coordinated-shutdown sequence proceeds to
// PhaseClusterLeave.
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

	timeout := r.shardSettings.HandoffTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	select {
	case <-r.handoffDone:
		r.Log().Info("Handoff Completed", "region", r.Self().Path())
	case <-time.After(timeout):
		r.Log().Warn("ShardRegion handoff timed out — proceeding without coordinator ack",
			"region", r.Self().Path(), "timeout", timeout)
	}
}

func (r *ShardRegion) Receive(msg any) {
	r.Log().Debug("Region received message", "type", fmt.Sprintf("%T", msg))
	switch m := msg.(type) {
	case ShardingEnvelope:
		shardId := m.ShardId
		if shardId == "" && m.EntityId != "" {
			// EntityRef.Tell wraps messages without a ShardId — derive it now.
			_, shardId, _ = r.extractEntityId(m.EntityId)
			m.ShardId = shardId
		}
		r.Log().Debug("Routing ShardingEnvelope", "shardId", shardId, "entityId", m.EntityId)
		r.deliverMessageWithSender(shardId, m, r.Sender())

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

	case BeginHandOff:
		// Coordinator wants to rebalance this shard to a less-loaded region.
		// Tell the local shard (if any) to buffer incoming messages, clear
		// the home-path cache so new messages from other regions buffer here
		// too, then acknowledge to the coordinator.
		sid := m.ShardId
		r.handoffInProgress[sid] = struct{}{}
		delete(r.shardHomePaths, sid)
		r.Log().Info("ShardRegion: BeginHandOff received", "shardId", sid)
		if shard, ok := r.shards[sid]; ok {
			shard.Tell(ShardBeginHandoff{ShardId: sid})
		}
		if r.coordinator != nil {
			r.coordinator.Tell(BeginHandOffAck{ShardId: sid}, r.Self())
		}

	case HandOff:
		// Coordinator authorises the entity shutdown.  Ask the local shard to
		// flush its stash buffer back to this region first (drain protocol),
		// then stop it.  ShardDrainResponse triggers the actual stop and the
		// ShardStopped notification to the coordinator.
		// If no local shard exists, notify the coordinator immediately.
		sid := m.ShardId
		if shard, ok := r.shards[sid]; ok {
			r.drainPending[sid] = struct{}{}
			shard.Tell(ShardDrainRequest{ShardId: sid, RegionRef: r.Self()})
			r.Log().Info("ShardRegion: HandOff — drain requested", "shardId", sid)
		} else {
			delete(r.handoffInProgress, sid)
			r.Log().Info("ShardRegion: HandOff — no local shard, notifying coordinator", "shardId", sid)
			if r.coordinator != nil {
				r.coordinator.Tell(ShardStopped{ShardId: sid}, r.Self())
			}
		}

	case ShardDrainResponse:
		// Shard has flushed its stash; stashed envelopes have been re-sent to
		// this region as plain ShardingEnvelope messages and will be buffered
		// in pendingMessages (home path is already cleared).  Now stop the
		// shard and notify the coordinator.
		sid := m.ShardId
		if shard, ok := r.shards[sid]; ok {
			if stopper, ok := r.System().(interface{ Stop(actor.Ref) }); ok {
				stopper.Stop(shard)
			}
			delete(r.shards, sid)
		}
		delete(r.drainPending, sid)
		delete(r.handoffInProgress, sid)
		r.Log().Info("ShardRegion: drain complete, shard stopped", "shardId", sid)
		if r.coordinator != nil {
			r.coordinator.Tell(ShardStopped{ShardId: sid}, r.Self())
		}

	case retryShardHomeTickMsg:
		r.retryPendingHomes()
		r.scheduleRetryTick()

	case actor.TerminatedMessage:
		// Handle shard termination or coordinator termination
		terminated := m.TerminatedActor()
		for sid, shard := range r.shards {
			if shard.Path() == terminated.Path() {
				delete(r.shards, sid)
				r.Log().Debug("Shard terminated", "shardId", sid)
				// Apply shard-failure-backoff: drop the cached home so further
				// messages for this shard buffer (and retry-interval re-asks the
				// coordinator) only after the configured delay. Without a delay,
				// pending messages would immediately retry against the coordinator
				// at full retry-interval cadence.
				if backoff := r.shardSettings.ShardFailureBackoff; backoff > 0 {
					sidCopy := sid
					self := r.Self()
					time.AfterFunc(backoff, func() {
						if self != nil {
							self.Tell(shardFailureBackoffElapsedMsg{ShardId: sidCopy})
						}
					})
				} else {
					delete(r.shardHomePaths, sid)
				}
				break
			}
		}
		if r.coordinator != nil && r.coordinator.Path() == terminated.Path() {
			r.Log().Error("ShardCoordinator terminated")
			// coordinator-failure-backoff: track elapsed time before allowing
			// re-registration retries to fire. The proxy/coordinator re-creation
			// path is owned by the singleton manager; we just record the delay.
			if backoff := r.shardSettings.CoordinatorFailureBackoff; backoff > 0 {
				self := r.Self()
				time.AfterFunc(backoff, func() {
					if self != nil {
						self.Tell(coordinatorFailureBackoffElapsedMsg{})
					}
				})
			}
		}

	case shardFailureBackoffElapsedMsg:
		// shard-failure-backoff has elapsed; clear the cached home so the next
		// pending message re-resolves via the coordinator.
		delete(r.shardHomePaths, m.ShardId)
		r.Log().Debug("shard-failure-backoff elapsed; cleared home cache", "shardId", m.ShardId)

	case coordinatorFailureBackoffElapsedMsg:
		// coordinator-failure-backoff has elapsed. Re-register with the
		// (possibly re-elected) coordinator if one is currently known.
		if r.coordinator != nil {
			r.Log().Debug("coordinator-failure-backoff elapsed; re-registering with coordinator")
			r.coordinator.Tell(RegisterRegion{RegionPath: r.Self().Path()}, r.Self())
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
				TraceContext:    injectTraceContext(context.Background()),
			}
			r.deliverMessageWithSender(shardId, envelope, r.Sender())
		}
	}
}

// injectTraceContext extracts the current span context from ctx and encodes it
// as W3C TraceContext headers using the global telemetry Tracer's Inject method.
// Returns an empty map when no active span is present.
func injectTraceContext(ctx context.Context) map[string]string {
	carrier := make(map[string]string)
	telemetry.GetTracer("github.com/sopranoworks/gekka/cluster/sharding").Inject(ctx, carrier)
	return carrier
}

func (r *ShardRegion) deliverMessageWithSender(shardId ShardId, envelope ShardingEnvelope, sender actor.Ref) {
	homePath, ok := r.shardHomePaths[shardId]
	if !ok || homePath == "" {
		// Ask coordinator for home, buffering this message until the response.
		// Enforce the buffer-size cap (pekko.cluster.sharding.buffer-size): once
		// we hit the cap for this shard, drop further messages with a debug log
		// rather than letting the queue grow unbounded.
		queue := r.pendingMessages[shardId]
		cap := r.shardSettings.BufferSize
		if cap > 0 && len(queue) >= cap {
			r.Log().Debug("buffer-size cap reached, dropping message",
				"shardId", shardId, "cap", cap, "queueLen", len(queue))
			return
		}
		r.Log().Debug("Requesting shard home", "shardId", shardId)
		r.pendingMessages[shardId] = append(queue, actor.Envelope{Payload: envelope, Sender: sender})
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

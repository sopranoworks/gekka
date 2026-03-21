/*
 * coordinator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// rebalanceTick is a periodic self-message that triggers a rebalance check.
type rebalanceTick struct{}

type ShardCoordinator struct {
	actor.BaseActor
	strategy ShardAllocationStrategy
	regions  map[string]actor.Ref    // RegionPath -> Ref
	shards   map[ShardId]string      // ShardId -> RegionPath
	rebalanceInProgress map[ShardId]struct{} // shards currently being rebalanced
	// RebalanceInterval overrides the default 10 s period between rebalance
	// checks.  Zero means 10 s.  Exposed for testing (set before PreStart).
	RebalanceInterval time.Duration
}

func NewShardCoordinator(strategy ShardAllocationStrategy) *ShardCoordinator {
	return &ShardCoordinator{
		BaseActor:           actor.NewBaseActor(),
		strategy:            strategy,
		regions:             make(map[string]actor.Ref),
		shards:              make(map[ShardId]string),
		rebalanceInProgress: make(map[ShardId]struct{}),
	}
}

// PreStart schedules the first periodic rebalance check.
func (c *ShardCoordinator) PreStart() {
	c.scheduleRebalanceTick()
}

// scheduleRebalanceTick arms a one-shot timer that delivers rebalanceTick to
// this actor's mailbox.  The handler re-arms the timer so the check repeats.
func (c *ShardCoordinator) scheduleRebalanceTick() {
	interval := c.RebalanceInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	self := c.Self()
	if self == nil {
		return // not yet registered (unit tests that skip PreStart)
	}
	time.AfterFunc(interval, func() { self.Tell(rebalanceTick{}) })
}

// doRebalance asks the strategy which shards to move and sends BeginHandOff to
// each owning region.
func (c *ShardCoordinator) doRebalance() {
	// Build the per-region allocation map for the strategy.
	regionMap := make(map[actor.Ref][]ShardId)
	for path, ref := range c.regions {
		regionMap[ref] = nil
		for sid, rpath := range c.shards {
			if rpath == path {
				regionMap[ref] = append(regionMap[ref], sid)
			}
		}
	}

	// Convert rebalanceInProgress to a slice for the strategy signature.
	inProgress := make([]ShardId, 0, len(c.rebalanceInProgress))
	for sid := range c.rebalanceInProgress {
		inProgress = append(inProgress, sid)
	}

	toMove := c.strategy.Rebalance(regionMap, inProgress)
	for _, sid := range toMove {
		if _, alreadyMoving := c.rebalanceInProgress[sid]; alreadyMoving {
			continue
		}
		ownerPath, found := c.shards[sid]
		if !found {
			continue
		}
		ownerRef, exists := c.regions[ownerPath]
		if !exists {
			continue
		}
		c.rebalanceInProgress[sid] = struct{}{}
		ownerRef.Tell(BeginHandOff{ShardId: sid}, c.Self())
		c.Log().Info("Rebalance: initiated handoff", "shardId", sid, "from", ownerPath)
	}
}

func (c *ShardCoordinator) Receive(msg any) {
	c.Log().Debug("Coordinator received message", "type", fmt.Sprintf("%T", msg))
	switch m := msg.(type) {
	case RegisterRegion:
		c.regions[m.RegionPath] = c.Sender()
		c.Log().Info("Region registered", "region", m.RegionPath)
		// Watch the region to handle node failures
		c.System().Watch(c.Self(), c.Sender())

	case GetShardHome:
		c.Log().Debug("Handling GetShardHome", "shardId", m.ShardId, "sender", func() string {
			if c.Sender() != nil {
				return c.Sender().Path()
			}
			return "<nil>"
		}())
		regionPath, ok := c.shards[m.ShardId]
		if !ok {
			c.Log().Debug("Allocating new shard", "shardId", m.ShardId)
			// Map regions for strategy
			regionMap := make(map[actor.Ref][]ShardId)
			for path, ref := range c.regions {
				regionMap[ref] = []ShardId{}
				for sid, rpath := range c.shards {
					if rpath == path {
						regionMap[ref] = append(regionMap[ref], sid)
					}
				}
			}

			region := c.strategy.AllocateShard(c.Sender(), m.ShardId, regionMap)
			if region != nil {
				regionPath = region.Path()
				c.shards[m.ShardId] = regionPath
				c.Log().Info("Shard allocated", "shardId", m.ShardId, "region", regionPath)
			} else {
				c.Log().Warn("Failed to allocate shard (no regions available)", "shardId", m.ShardId)
			}
		}
		c.Sender().Tell(ShardHome{ShardId: m.ShardId, RegionPath: regionPath}, c.Self())

	case RegionHandoffRequest:
		// A region is shutting down gracefully.  Release all shards it owns so
		// the coordinator can reallocate them to surviving regions, then
		// acknowledge with HandoffComplete so the departing region's PostStop
		// can unblock and the coordinated-shutdown sequence can proceed.
		regionPath := m.RegionPath
		c.Log().Info("Region handoff requested", "region", regionPath)
		released := 0
		for sid, rpath := range c.shards {
			if rpath == regionPath {
				delete(c.shards, sid)
				released++
			}
		}
		delete(c.regions, regionPath)
		c.Log().Info("Handoff complete: shards released",
			"region", regionPath, "released", released)
		c.Sender().Tell(HandoffComplete{RegionPath: regionPath}, c.Self())

	case rebalanceTick:
		c.doRebalance()
		c.scheduleRebalanceTick()

	case BeginHandOffAck:
		// Region has acknowledged BeginHandOff and is ready to drain.
		// Send HandOff to authorise the entity shutdown.
		ownerPath, found := c.shards[m.ShardId]
		if !found {
			c.Log().Warn("BeginHandOffAck for unknown shard", "shardId", m.ShardId)
			return
		}
		if ownerRef, ok := c.regions[ownerPath]; ok {
			ownerRef.Tell(HandOff{ShardId: m.ShardId}, c.Self())
			c.Log().Info("Rebalance: HandOff sent to region", "shardId", m.ShardId, "region", ownerPath)
		}

	case ShardStopped:
		// Region has drained the shard; clear allocation so the next
		// GetShardHome re-assigns it to a less-loaded region.
		delete(c.rebalanceInProgress, m.ShardId)
		delete(c.shards, m.ShardId)
		c.Log().Info("Rebalance: shard cleared for reallocation", "shardId", m.ShardId)

	case actor.TerminatedMessage:
		// Region stopped unexpectedly (node crash / process kill).
		// Clean up without sending HandoffComplete — the region is already gone.
		terminated := m.TerminatedActor()
		terminatedPath := terminated.Path()
		if _, ok := c.regions[terminatedPath]; ok {
			c.Log().Info("Region terminated unexpectedly, removing shards", "region", terminatedPath)
			for sid, rpath := range c.shards {
				if rpath == terminatedPath {
					delete(c.shards, sid)
					delete(c.rebalanceInProgress, sid)
				}
			}
			delete(c.regions, terminatedPath)
		}
	}
}

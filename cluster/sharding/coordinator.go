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

	"github.com/sopranoworks/gekka/actor"
)

type ShardCoordinator struct {
	actor.BaseActor
	strategy ShardAllocationStrategy
	regions  map[string]actor.Ref // RegionPath -> Ref
	shards   map[ShardId]string   // ShardId -> RegionPath
}

func NewShardCoordinator(strategy ShardAllocationStrategy) *ShardCoordinator {
	return &ShardCoordinator{
		BaseActor: actor.NewBaseActor(),
		strategy:  strategy,
		regions:   make(map[string]actor.Ref),
		shards:    make(map[ShardId]string),
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
				}
			}
			delete(c.regions, terminatedPath)
		}
	}
}

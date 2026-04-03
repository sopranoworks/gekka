/*
 * sharded_daemon.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"strconv"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// ShardedDaemonProcessSettings controls how daemon workers are distributed.
type ShardedDaemonProcessSettings struct {
	// ShardSettings are forwarded to the underlying ShardRegion.
	ShardSettings ShardSettings

	// Role restricts the coordinator singleton to nodes carrying this cluster
	// role.  An empty string means "any node".
	Role string
}

// ShardedDaemonProcess distributes numberOfInstances background workers across
// the cluster using cluster sharding.  Each worker is identified by an integer
// index in [0, numberOfInstances) and is guaranteed to run on exactly one node
// at any time.  If the hosting node leaves, the cluster reshards and restarts
// the worker on a surviving node.
//
// This mirrors Pekko's pekko.cluster.sharding.typed.ShardedDaemonProcess.
//
// Usage:
//
//	sdp, err := sharding.InitShardedDaemonProcess(sys, cm, router, "my-daemons",
//	    4, func(idx int) actor.Actor { return &MyDaemon{Index: idx} },
//	    sharding.ShardedDaemonProcessSettings{})
//
//	// sdp.Ref is the local ShardRegion — you can send messages to specific
//	// daemons using sdp.Tell(idx, msg).
type ShardedDaemonProcess struct {
	// Region is the local ShardRegion actor ref.
	Region actor.Ref

	// NumberOfInstances is the total number of daemons in the process.
	NumberOfInstances int
}

// Tell sends msg to the daemon with the given zero-based index.
func (d *ShardedDaemonProcess) Tell(index int, msg any) {
	if index < 0 || index >= d.NumberOfInstances {
		return
	}
	// Route through daemonEnvelope so the region's extractor can derive the
	// entityId and shardId from the index.
	d.Region.Tell(daemonEnvelope{index: index, payload: msg})
}

// daemonEnvelope is an internal message used to route application messages to
// a specific daemon by index without polluting the public API.
type daemonEnvelope struct {
	index   int
	payload any
}

// daemonStart is the bootstrap message sent to each daemon entity on startup.
// The entity can pattern-match on this type to run initialisation logic.
type daemonStart struct{}

// DaemonStart is exported so that daemon actor Receive methods can pattern-
// match on the startup signal:
//
//	case sharding.DaemonStart:
//	    // run initialisation
var DaemonStart = daemonStart{}

// InitShardedDaemonProcess starts cluster sharding for numberOfInstances
// background daemon actors and sends each one a DaemonStart bootstrap signal.
//
// behaviorFactory is called once per entity creation with the zero-based
// daemon index.
//
// The function returns a *ShardedDaemonProcess whose Region ref can be used
// to send application messages to individual daemons.
func InitShardedDaemonProcess(
	sys actor.ActorContext,
	cm *cluster.ClusterManager,
	router cluster.Router,
	name string,
	numberOfInstances int,
	behaviorFactory func(index int) actor.Actor,
	settings ShardedDaemonProcessSettings,
) (*ShardedDaemonProcess, error) {
	if numberOfInstances <= 0 {
		return nil, fmt.Errorf("sharded daemon process: numberOfInstances must be > 0, got %d", numberOfInstances)
	}
	if behaviorFactory == nil {
		return nil, fmt.Errorf("sharded daemon process: behaviorFactory must not be nil")
	}

	typeName := "ShardedDaemonProcess-" + name

	// extractor converts a daemonEnvelope (or plain daemonStart) to
	// (entityId, shardId, payload).  The shard ID is derived from the entity
	// index modulo numberOfInstances so that all N daemons map to unique shards.
	extract := func(msg any) (EntityId, ShardId, any) {
		switch m := msg.(type) {
		case daemonEnvelope:
			id := strconv.Itoa(m.index)
			return id, strconv.Itoa(m.index % numberOfInstances), m.payload
		case daemonStart:
			// Should not arrive through normal routing; return sentinel.
			return "", "", nil
		default:
			return "", "", nil
		}
	}

	cfg := ClusterShardingConfig{
		TypeName: typeName,
		EntityProps: EntityProps{
			New: func(entityId EntityId) actor.Actor {
				idx, err := strconv.Atoi(entityId)
				if err != nil {
					// Fallback: create daemon 0.
					idx = 0
				}
				return behaviorFactory(idx)
			},
		},
		Settings:  settings.ShardSettings,
		Extractor: extract,
		Role:      settings.Role,
	}

	regionRef, err := StartSharding(sys, cm, router, cfg)
	if err != nil {
		return nil, fmt.Errorf("sharded daemon process %q: %w", name, err)
	}

	// Bootstrap: send a DaemonStart to every daemon entity so they initialise.
	// Each daemon's actor is spawned lazily by the region on first message.
	for i := 0; i < numberOfInstances; i++ {
		regionRef.Tell(daemonEnvelope{index: i, payload: DaemonStart})
	}

	return &ShardedDaemonProcess{
		Region:            regionRef,
		NumberOfInstances: numberOfInstances,
	}, nil
}

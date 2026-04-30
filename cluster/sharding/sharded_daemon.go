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
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// defaultShardedDaemonKeepAliveNanos backs the package-level default that
// pekko.cluster.sharded-daemon-process.keep-alive-interval feeds. Stored as
// nanoseconds for lock-free atomic.Int64 reads. Zero ⇒ 10s (Pekko default).
var defaultShardedDaemonKeepAliveNanos atomic.Int64

// defaultShardedDaemonRole backs the package-level default that
// pekko.cluster.sharded-daemon-process.sharding.role feeds. Empty ⇒ no role
// override; the underlying ShardRegion runs on any node.
var defaultShardedDaemonRole atomic.Value // string

// SetDefaultShardedDaemonProcessKeepAliveInterval installs the default keep-
// alive cadence used by InitShardedDaemonProcess when callers leave
// ShardedDaemonProcessSettings.KeepAliveInterval unset (zero). Non-positive
// values revert to the 10s Pekko default.
//
// HOCON: pekko.cluster.sharded-daemon-process.keep-alive-interval (default 10s).
func SetDefaultShardedDaemonProcessKeepAliveInterval(d time.Duration) {
	if d <= 0 {
		defaultShardedDaemonKeepAliveNanos.Store(0)
		return
	}
	defaultShardedDaemonKeepAliveNanos.Store(int64(d))
}

// GetDefaultShardedDaemonProcessKeepAliveInterval returns the currently
// configured default keep-alive cadence. Falls back to 10s when unconfigured.
func GetDefaultShardedDaemonProcessKeepAliveInterval() time.Duration {
	v := defaultShardedDaemonKeepAliveNanos.Load()
	if v <= 0 {
		return 10 * time.Second
	}
	return time.Duration(v)
}

// SetDefaultShardedDaemonProcessShardingRole installs the default role used
// by InitShardedDaemonProcess when callers leave
// ShardedDaemonProcessSettings.Role empty. An empty string clears the
// override (any node).
//
// HOCON: pekko.cluster.sharded-daemon-process.sharding.role.
func SetDefaultShardedDaemonProcessShardingRole(role string) {
	defaultShardedDaemonRole.Store(role)
}

// GetDefaultShardedDaemonProcessShardingRole returns the currently configured
// default role override. Empty when unconfigured.
func GetDefaultShardedDaemonProcessShardingRole() string {
	v, _ := defaultShardedDaemonRole.Load().(string)
	return v
}

// ShardedDaemonProcessSettings controls how daemon workers are distributed.
type ShardedDaemonProcessSettings struct {
	// ShardSettings are forwarded to the underlying ShardRegion.
	ShardSettings ShardSettings

	// Role restricts the coordinator singleton to nodes carrying this cluster
	// role.  An empty string means "any node".  Mirrors
	// pekko.cluster.sharded-daemon-process.sharding.role.
	Role string

	// KeepAliveInterval, when > 0, overrides the package-level default
	// (pekko.cluster.sharded-daemon-process.keep-alive-interval; 10s) for
	// this process. The configured value drives a periodic ping that
	// re-sends DaemonStart to every entity index, ensuring entities that
	// stopped (passivation, rebalance, crash) are reignited on the next
	// tick. Zero falls back to GetDefaultShardedDaemonProcessKeepAliveInterval().
	KeepAliveInterval time.Duration
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

	// stopCh signals the keep-alive goroutine to exit. nil when the
	// process was constructed without a keep-alive loop (e.g. tests
	// holding ShardedDaemonProcess directly).
	stopCh chan struct{}
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

// Stop terminates the keep-alive ping loop spawned by InitShardedDaemonProcess.
// Safe to call multiple times. Does not stop the underlying ShardRegion.
func (d *ShardedDaemonProcess) Stop() {
	if d == nil || d.stopCh == nil {
		return
	}
	select {
	case <-d.stopCh:
		// already closed
	default:
		close(d.stopCh)
	}
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

	// Resolve the effective role: explicit setting wins, otherwise inherit the
	// HOCON-fed package default (pekko.cluster.sharded-daemon-process.sharding.role).
	effectiveRole := settings.Role
	if effectiveRole == "" {
		effectiveRole = GetDefaultShardedDaemonProcessShardingRole()
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
		Role:      effectiveRole,
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

	// Resolve the effective keep-alive cadence from per-process override or
	// the HOCON-fed package default. Drives a goroutine that re-issues
	// DaemonStart so passivated/rebalanced/crashed entities are reignited.
	keepAlive := settings.KeepAliveInterval
	if keepAlive <= 0 {
		keepAlive = GetDefaultShardedDaemonProcessKeepAliveInterval()
	}

	stopCh := make(chan struct{})
	go runKeepAliveLoop(regionRef, numberOfInstances, keepAlive, stopCh)

	return &ShardedDaemonProcess{
		Region:            regionRef,
		NumberOfInstances: numberOfInstances,
		stopCh:            stopCh,
	}, nil
}

// runKeepAliveLoop pings every daemon entity index at the configured cadence
// until stopCh closes. Each tick sends a DaemonStart envelope to the region
// for every i in [0, numberOfInstances). Exposed for direct testing.
func runKeepAliveLoop(region actor.Ref, numberOfInstances int, interval time.Duration, stopCh <-chan struct{}) {
	if interval <= 0 || numberOfInstances <= 0 || region == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			for i := 0; i < numberOfInstances; i++ {
				region.Tell(daemonEnvelope{index: i, payload: DaemonStart})
			}
		}
	}
}

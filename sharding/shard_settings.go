/*
 * shard_settings.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"time"

	"github.com/sopranoworks/gekka/persistence"
)

// ShardSettings holds per-shard advanced configuration for passivation,
// remember-entities, and multi-DC routing.  Pass it through ShardingSettings
// when calling StartSharding.
type ShardSettings struct {
	// PassivationIdleTimeout, when > 0, automatically stops an entity that
	// has not received a message within this duration.  The shard checks
	// for idle entities roughly every (PassivationIdleTimeout / 4), with a
	// minimum check interval of 500 ms.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.passivation.idle-timeout = 2m
	PassivationIdleTimeout time.Duration

	// RememberEntities, when true, persists EntityStarted / EntityStopped
	// events to the Journal so entities are re-spawned automatically when
	// the Shard restarts.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.remember-entities = on
	RememberEntities bool

	// Journal is the persistence store used when RememberEntities is true.
	// If nil and RememberEntities is true, a fresh InMemoryJournal is used
	// (suitable for tests; use a durable backend in production).
	Journal persistence.Journal

	// DataCenter restricts this ShardRegion to accepting only local-DC
	// entities.  When non-empty, messages destined for shards on a remote DC
	// are forwarded without applying local-shard caching.
	DataCenter string

	// IsLocalDC is a predicate that returns true when the node with the given
	// host:port is in the same data center as this node.  Populated by
	// StartSharding when DataCenter is set; leave nil if not using multi-DC.
	IsLocalDC func(host string, port uint32) bool

	// HandoffTimeout is the maximum duration ShardRegion.PostStop waits for
	// a HandoffComplete acknowledgement from the ShardCoordinator before
	// proceeding with shutdown.  When zero the default of 10 seconds applies.
	//
	// Equivalent HOCON key:
	//   gekka.cluster.sharding.handoff-timeout = 10s
	HandoffTimeout time.Duration
}

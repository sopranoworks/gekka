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

	// Store, when non-nil, is used for remember-entities instead of the
	// event-sourced Journal.  ShardStore tracks the current set of live
	// entity IDs directly: passivation does NOT remove an entity from the
	// store, so all entities are re-spawned on shard restart regardless of
	// whether they were passivated before the crash.  An entity is removed
	// from the store only on explicit termination (actor.TerminatedMessage).
	//
	// When Store is nil and RememberEntities is true, the Journal-based
	// event-sourcing path is used instead.
	Store ShardStore

	// JournalStorePath is the filesystem path for a FileStore that is
	// auto-created when Store is nil and RememberEntities is true.
	// Ignored when Store is set explicitly.
	JournalStorePath string

	// HandoffTimeout is the maximum duration ShardRegion.PostStop waits for
	// a HandoffComplete acknowledgement from the ShardCoordinator before
	// proceeding with shutdown.  When zero the default of 10 seconds applies.
	//
	// Equivalent HOCON key:
	//   gekka.cluster.sharding.handoff-timeout = 10s
	HandoffTimeout time.Duration

	// NumberOfShards is the total number of shards distributed across the
	// cluster.  This value is used by HashCodeExtractor to compute shard IDs
	// from entity IDs (entityId.hashCode % numberOfShards).  Once sharding
	// has started, this value must not change.  Defaults to 1000 when zero.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.number-of-shards = 1000
	NumberOfShards int

	// GuardianName is the actor name for the sharding guardian.
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.guardian-name = "sharding"
	GuardianName string

	// PassivationStrategy selects the passivation strategy.
	// "default-idle-strategy" (idle timeout) or "custom-lru-strategy" (LRU eviction).
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.passivation.strategy = "default-idle-strategy"
	PassivationStrategy string

	// PassivationActiveEntityLimit is the max active entities for LRU strategy.
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.passivation.custom-lru-strategy.active-entity-limit = 100000
	PassivationActiveEntityLimit int
}

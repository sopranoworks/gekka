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

	// RetryInterval is the period between retries of GetShardHome requests
	// for shards whose home is still unknown. When zero, retries are
	// disabled and the region waits for an unsolicited ShardHome push.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.retry-interval = 2s
	RetryInterval time.Duration

	// BufferSize caps the per-shard pending-message queue used by
	// ShardRegion while waiting for a ShardHome reply from the coordinator.
	// Once the cap is reached, additional messages for that shard are
	// dropped. Zero means unbounded (legacy behavior).
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.buffer-size = 100000
	BufferSize int

	// ShardStartTimeout is the maximum time a Shard waits during its own
	// startup (e.g. recovering remember-entities state) before giving up.
	// Zero falls back to 10 seconds.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.shard-start-timeout = 10s
	ShardStartTimeout time.Duration

	// ShardFailureBackoff is the delay before a Shard actor that has
	// terminated is allowed to be re-spawned by its ShardRegion.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.shard-failure-backoff = 10s
	ShardFailureBackoff time.Duration

	// EntityRestartBackoff is the delay before a terminated entity actor
	// inside a Shard is allowed to be re-spawned (typically when
	// remember-entities is enabled and the entity exited unexpectedly).
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.entity-restart-backoff = 10s
	EntityRestartBackoff time.Duration

	// CoordinatorFailureBackoff is the delay before the
	// ShardCoordinatorProxy retries reaching the coordinator singleton
	// after a transient failure.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.coordinator-failure-backoff = 5s
	CoordinatorFailureBackoff time.Duration

	// WaitingForStateTimeout caps how long the Shard waits for the initial
	// distributed state read to complete during recovery. Plumbed onto
	// ShardSettings; consumed by the DData-backed coordinator state path
	// when present.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.waiting-for-state-timeout = 2s
	WaitingForStateTimeout time.Duration

	// UpdatingStateTimeout caps how long the Shard waits for a distributed
	// state update (or a remember-entities write) to complete before retrying.
	// Plumbed onto ShardSettings; consumed by the DData-backed update path
	// when present.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.updating-state-timeout = 5s
	UpdatingStateTimeout time.Duration

	// ShardRegionQueryTimeout caps how long the ShardRegion waits when
	// answering a query that needs to reach every shard. Plumbed onto
	// ShardSettings; consumed by region-level query handlers as they are
	// added.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.shard-region-query-timeout = 3s
	ShardRegionQueryTimeout time.Duration

	// EntityRecoveryStrategy selects how a Shard re-spawns remembered
	// entities during recovery. "all" (the default) starts every entity at
	// once. "constant" paces recovery: it spawns
	// EntityRecoveryConstantRateNumberOfEntities entities, waits
	// EntityRecoveryConstantRateFrequency, then repeats until all
	// remembered entities are back.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.entity-recovery-strategy = "all"
	EntityRecoveryStrategy string

	// EntityRecoveryConstantRateFrequency is the delay between successive
	// entity-spawn batches when EntityRecoveryStrategy = "constant".
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency = 100ms
	EntityRecoveryConstantRateFrequency time.Duration

	// EntityRecoveryConstantRateNumberOfEntities is the batch size for
	// constant-rate recovery. Zero falls back to 5 (Pekko default).
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities = 5
	EntityRecoveryConstantRateNumberOfEntities int

	// CoordinatorWriteMajorityPlus is the additional number of nodes (above
	// majority) that DData writes for coordinator state must reach. May be
	// math.MaxInt to mean "all nodes" in keeping with Pekko's "all"
	// special-value. Plumbed onto ShardSettings; consumed by the DData
	// coordinator state path when present.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.coordinator-state.write-majority-plus = 3
	CoordinatorWriteMajorityPlus int

	// CoordinatorReadMajorityPlus is the additional number of nodes (above
	// majority) that DData reads for coordinator state must reach.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.coordinator-state.read-majority-plus = 5
	CoordinatorReadMajorityPlus int

	// VerboseDebugLogging gates fine-grained per-message DEBUG-level
	// log lines emitted by the Shard / ShardRegion. When false, only
	// coarse-grained debug lines (state transitions, errors) are logged.
	// Be careful enabling in production.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.verbose-debug-logging = off
	VerboseDebugLogging bool

	// FailOnInvalidEntityStateTransition, when true, panics the Shard on
	// an invalid internal state-machine transition instead of logging a
	// warning. Mostly used by the project's own test suite.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.fail-on-invalid-entity-state-transition = off
	FailOnInvalidEntityStateTransition bool

	// IdleEntityCheckInterval overrides the cadence at which the Shard
	// scans for idle entities to passivate. When zero, the cadence falls
	// back to PassivationIdleTimeout / 2 (Pekko's "default") with a
	// minimum of 500 ms.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval
	IdleEntityCheckInterval time.Duration
}

// EntityRecoveryStrategyAll is the default entity-recovery strategy: every
// remembered entity is re-spawned at once during shard recovery.
const EntityRecoveryStrategyAll = "all"

// EntityRecoveryStrategyConstant paces shard recovery by spawning a fixed
// number of entities per tick of EntityRecoveryConstantRateFrequency.
const EntityRecoveryStrategyConstant = "constant"

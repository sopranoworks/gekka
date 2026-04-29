/*
 * sharding.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/sharding"
	styped "github.com/sopranoworks/gekka/cluster/sharding/typed"
	icluster "github.com/sopranoworks/gekka/internal/cluster"
	"github.com/sopranoworks/gekka/persistence"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
)

// ShardingSettings defines configuration for cluster sharding.
type ShardingSettings struct {
	// Role restricts sharding to nodes with this cluster role.
	Role string

	// NumberOfShards is the total number of shards (immutable after start).
	NumberOfShards int

	// AllocationStrategy decides which node hosts a given shard.
	// Defaults to LeastShardAllocationStrategy(threshold=3, maxSimultaneous=1).
	AllocationStrategy sharding.ShardAllocationStrategy

	// PassivationIdleTimeout, when > 0, automatically stops entities idle
	// longer than this duration.
	//
	// HOCON: pekko.cluster.sharding.passivation.idle-timeout
	PassivationIdleTimeout time.Duration

	// RememberEntities, when true, persists entity lifecycle events so
	// entities are re-spawned after a Shard restart or failover.
	//
	// HOCON: pekko.cluster.sharding.remember-entities = on
	RememberEntities bool

	// Journal is used when RememberEntities is true.  Defaults to an
	// InMemoryJournal if nil (use a durable backend in production).
	Journal persistence.Journal

	// DataCenter restricts shard allocation to nodes in this data center.
	// Leave empty to allow placement on any node.
	//
	// HOCON equivalent:
	//   pekko.cluster.multi-data-center.self-data-center = "us-east"
	DataCenter string

	// HandoffTimeout is the maximum time a ShardRegion waits for the
	// coordinator to acknowledge shard handoff during coordinated shutdown.
	// Larger clusters or heavily loaded coordinators may need a longer value.
	// Defaults to 10 seconds when zero or unset.
	//
	// HOCON: gekka.cluster.sharding.handoff-timeout
	HandoffTimeout time.Duration

	// GuardianName is the actor name for the sharding guardian.
	// Corresponds to pekko.cluster.sharding.guardian-name. Default: "sharding".
	GuardianName string

	// RememberEntitiesStore selects the backend: "eventsourced" or "ddata".
	// Corresponds to pekko.cluster.sharding.remember-entities-store. Default: "ddata".
	RememberEntitiesStore string

	// PassivationStrategy selects the passivation strategy.
	// "default-idle-strategy" (idle timeout) or "custom-lru-strategy" (LRU eviction).
	// Corresponds to pekko.cluster.sharding.passivation.strategy. Default: "default-idle-strategy".
	PassivationStrategy string

	// PassivationActiveEntityLimit is the max active entities for LRU strategy.
	// Corresponds to pekko.cluster.sharding.passivation.custom-lru-strategy.active-entity-limit.
	// Default: 100000.
	PassivationActiveEntityLimit int

	// RetryInterval is the period between retries of GetShardHome requests
	// for shards whose home is still unknown.
	// Corresponds to pekko.cluster.sharding.retry-interval. Pekko default: 2s.
	RetryInterval time.Duration

	// BufferSize caps the per-shard pending-message queue while waiting
	// for a ShardHome reply.
	// Corresponds to pekko.cluster.sharding.buffer-size. Pekko default: 100000.
	BufferSize int

	// ShardStartTimeout is the maximum time a Shard waits during startup.
	// Corresponds to pekko.cluster.sharding.shard-start-timeout. Pekko default: 10s.
	ShardStartTimeout time.Duration

	// ShardFailureBackoff is the delay before a terminated Shard is re-spawned.
	// Corresponds to pekko.cluster.sharding.shard-failure-backoff. Pekko default: 10s.
	ShardFailureBackoff time.Duration

	// EntityRestartBackoff is the delay before a terminated entity is re-spawned.
	// Corresponds to pekko.cluster.sharding.entity-restart-backoff. Pekko default: 10s.
	EntityRestartBackoff time.Duration

	// CoordinatorFailureBackoff is the delay before the coordinator proxy
	// retries after a transient failure.
	// Corresponds to pekko.cluster.sharding.coordinator-failure-backoff. Pekko default: 5s.
	CoordinatorFailureBackoff time.Duration

	// WaitingForStateTimeout caps how long the Shard waits for the initial
	// distributed-state read during recovery.
	// Corresponds to pekko.cluster.sharding.waiting-for-state-timeout. Pekko default: 2s.
	WaitingForStateTimeout time.Duration

	// UpdatingStateTimeout caps how long the Shard waits for a
	// distributed-state update / remember-entities write to complete.
	// Corresponds to pekko.cluster.sharding.updating-state-timeout. Pekko default: 5s.
	UpdatingStateTimeout time.Duration

	// ShardRegionQueryTimeout caps how long the ShardRegion waits when
	// answering a query that needs to reach every shard.
	// Corresponds to pekko.cluster.sharding.shard-region-query-timeout. Pekko default: 3s.
	ShardRegionQueryTimeout time.Duration

	// EntityRecoveryStrategy selects how a Shard re-spawns remembered
	// entities. "all" (default) spawns every entity at once; "constant"
	// spawns batches of EntityRecoveryConstantRateNumberOfEntities every
	// EntityRecoveryConstantRateFrequency.
	// Corresponds to pekko.cluster.sharding.entity-recovery-strategy.
	EntityRecoveryStrategy string

	// EntityRecoveryConstantRateFrequency is the delay between successive
	// entity-spawn batches under the "constant" strategy.
	// Corresponds to
	// pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency.
	// Pekko default: 100ms.
	EntityRecoveryConstantRateFrequency time.Duration

	// EntityRecoveryConstantRateNumberOfEntities is the batch size for the
	// "constant" entity-recovery strategy.
	// Corresponds to
	// pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities.
	// Pekko default: 5.
	EntityRecoveryConstantRateNumberOfEntities int

	// CoordinatorWriteMajorityPlus is the additional number of nodes (above
	// majority) DData writes for coordinator state must reach.
	// Corresponds to pekko.cluster.sharding.coordinator-state.write-majority-plus.
	// Pekko default: 3.
	CoordinatorWriteMajorityPlus int

	// CoordinatorReadMajorityPlus is the additional number of nodes (above
	// majority) DData reads for coordinator state must reach.
	// Corresponds to pekko.cluster.sharding.coordinator-state.read-majority-plus.
	// Pekko default: 5.
	CoordinatorReadMajorityPlus int

	// VerboseDebugLogging gates fine-grained per-message DEBUG logs in
	// the sharding code path.
	// Corresponds to pekko.cluster.sharding.verbose-debug-logging. Default: off.
	VerboseDebugLogging bool

	// FailOnInvalidEntityStateTransition, when true, makes the Shard
	// panic on an invalid internal state transition.
	// Corresponds to
	// pekko.cluster.sharding.fail-on-invalid-entity-state-transition. Default: off.
	FailOnInvalidEntityStateTransition bool

	// IdleEntityCheckInterval overrides the cadence of the idle-entity
	// passivation scan. When zero, defaults to PassivationIdleTimeout/2.
	// Corresponds to
	// pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval.
	IdleEntityCheckInterval time.Duration

	// Lease, when non-nil, is acquired by every Shard before becoming active
	// and released on shard handoff/stop.  When unset, StartSharding plumbs in
	// a lease resolved from pekko.cluster.sharding.use-lease.
	Lease icluster.Lease

	// LeaseRetryDelay is the backoff between Shard lease-acquisition retries
	// when a previous Acquire returns false or errors.  When zero StartSharding
	// falls back to ClusterConfig.Sharding.LeaseRetryInterval (default 5s).
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.lease-retry-interval = 5s
	LeaseRetryDelay time.Duration

	// ── Round-2 session 26 — composite (W-TinyLFU) passivation knobs ──
	//
	// PassivationWindowProportion is the admission-window size as a
	// fraction of PassivationActiveEntityLimit.  Used only by the
	// composite "default-strategy".  Pekko default: 0.01.
	PassivationWindowProportion float64
	// PassivationWindowPolicy names the window-area replacement policy.
	// Only "least-recently-used" is honoured at runtime today.
	PassivationWindowPolicy string
	// PassivationFilter selects the admission filter for the composite
	// strategy.  Recognised values: "frequency-sketch" (Pekko default),
	// "off", "none".
	PassivationFilter string
	// PassivationFrequencySketchDepth is the count-min-sketch depth.
	// Pekko default: 4.
	PassivationFrequencySketchDepth int
	// PassivationFrequencySketchCounterBits is documented for
	// forward-compat with Pekko configs; gekka stores 4-bit counters
	// regardless.  Pekko default: 4.
	PassivationFrequencySketchCounterBits int
	// PassivationFrequencySketchWidthMultiplier sets the sketch width
	// as active-entity-limit × multiplier.  Pekko default: 4.
	PassivationFrequencySketchWidthMultiplier int
	// PassivationFrequencySketchResetMultiplier governs the cadence of
	// the sketch's halving reset, expressed as a multiplier of the
	// active-entity-limit.  Pekko default: 10.0.
	PassivationFrequencySketchResetMultiplier float64

	// EventSourcedMaxUpdatesPerWrite caps the number of buffered
	// EntityStarted/EntityStopped events that are coalesced into a single
	// journal write under the eventsourced remember-entities backend.
	// When zero (and HOCON is also zero) the legacy one-event-per-write
	// path stays in effect.
	//
	// Equivalent HOCON key:
	//   pekko.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write = 100
	EventSourcedMaxUpdatesPerWrite int
}

// StartSharding starts cluster sharding for a given entity type.
// It returns an EntityRef that can be used to send messages to entities.
func StartSharding[Command any, Event any, State any](
	sys ActorSystem,
	typeName string,
	behaviorFactory func(entityId string) *ptyped.EventSourcedBehavior[Command, Event, State],
	extract sharding.ExtractEntityId,
	settings ShardingSettings,
) (sharding.ClusterEntityRef[Command], error) {

	// Register sharding types for serialization
	sys.RegisterType("sharding.RegisterRegion", reflect.TypeOf(sharding.RegisterRegion{}))
	sys.RegisterType("sharding.GetShardHome", reflect.TypeOf(sharding.GetShardHome{}))
	sys.RegisterType("sharding.ShardHome", reflect.TypeOf(sharding.ShardHome{}))
	sys.RegisterType("sharding.ShardingEnvelope", reflect.TypeOf(sharding.ShardingEnvelope{}))
	sys.RegisterType("sharding.ShardHomes", reflect.TypeOf(sharding.ShardHomes{}))
	sys.RegisterType("sharding.RegionHandoffRequest", reflect.TypeOf(sharding.RegionHandoffRequest{}))
	sys.RegisterType("sharding.HandoffComplete", reflect.TypeOf(sharding.HandoffComplete{}))
	sys.RegisterType("sharding.BeginHandOff", reflect.TypeOf(sharding.BeginHandOff{}))
	sys.RegisterType("sharding.BeginHandOffAck", reflect.TypeOf(sharding.BeginHandOffAck{}))
	sys.RegisterType("sharding.HandOff", reflect.TypeOf(sharding.HandOff{}))
	sys.RegisterType("sharding.ShardStopped", reflect.TypeOf(sharding.ShardStopped{}))
	sys.RegisterType("sharding.ShardBeginHandoff", reflect.TypeOf(sharding.ShardBeginHandoff{}))
	sys.RegisterType("sharding.ShardDrainRequest", reflect.TypeOf(sharding.ShardDrainRequest{}))
	sys.RegisterType("sharding.ShardDrainResponse", reflect.TypeOf(sharding.ShardDrainResponse{}))
	sys.RegisterType("sharding.RebalanceShard", reflect.TypeOf(sharding.RebalanceShard{}))
	sys.RegisterType("string", reflect.TypeOf(""))

	// Apply HOCON defaults from ClusterConfig when settings fields are zero.
	if cluster, ok := sys.(*Cluster); ok {
		if settings.NumberOfShards == 0 && cluster.cfg.Sharding.NumberOfShards > 0 {
			settings.NumberOfShards = cluster.cfg.Sharding.NumberOfShards
		}
		if settings.Role == "" && cluster.cfg.Sharding.Role != "" {
			settings.Role = cluster.cfg.Sharding.Role
		}
		if settings.PassivationIdleTimeout == 0 && cluster.cfg.Sharding.PassivationIdleTimeout > 0 {
			settings.PassivationIdleTimeout = cluster.cfg.Sharding.PassivationIdleTimeout
		}
		if settings.GuardianName == "" && cluster.cfg.Sharding.GuardianName != "" {
			settings.GuardianName = cluster.cfg.Sharding.GuardianName
		}
		if settings.RememberEntitiesStore == "" && cluster.cfg.Sharding.RememberEntitiesStore != "" {
			settings.RememberEntitiesStore = cluster.cfg.Sharding.RememberEntitiesStore
		}
		if settings.PassivationStrategy == "" && cluster.cfg.Sharding.PassivationStrategy != "" {
			settings.PassivationStrategy = cluster.cfg.Sharding.PassivationStrategy
		}
		if settings.PassivationActiveEntityLimit == 0 && cluster.cfg.Sharding.PassivationActiveEntityLimit > 0 {
			settings.PassivationActiveEntityLimit = cluster.cfg.Sharding.PassivationActiveEntityLimit
		}
		// Round-2 session 26 — composite (W-TinyLFU) knobs.  Each falls
		// back to the HOCON-loaded ClusterShardingConfig value when the
		// caller didn't override it directly on ShardingSettings.
		if settings.PassivationWindowProportion == 0 && cluster.cfg.Sharding.PassivationWindowProportion > 0 {
			settings.PassivationWindowProportion = cluster.cfg.Sharding.PassivationWindowProportion
		}
		if settings.PassivationWindowPolicy == "" && cluster.cfg.Sharding.PassivationWindowPolicy != "" {
			settings.PassivationWindowPolicy = cluster.cfg.Sharding.PassivationWindowPolicy
		}
		if settings.PassivationFilter == "" && cluster.cfg.Sharding.PassivationFilter != "" {
			settings.PassivationFilter = cluster.cfg.Sharding.PassivationFilter
		}
		if settings.PassivationFrequencySketchDepth == 0 && cluster.cfg.Sharding.PassivationFrequencySketchDepth > 0 {
			settings.PassivationFrequencySketchDepth = cluster.cfg.Sharding.PassivationFrequencySketchDepth
		}
		if settings.PassivationFrequencySketchCounterBits == 0 && cluster.cfg.Sharding.PassivationFrequencySketchCounterBits > 0 {
			settings.PassivationFrequencySketchCounterBits = cluster.cfg.Sharding.PassivationFrequencySketchCounterBits
		}
		if settings.PassivationFrequencySketchWidthMultiplier == 0 && cluster.cfg.Sharding.PassivationFrequencySketchWidthMultiplier > 0 {
			settings.PassivationFrequencySketchWidthMultiplier = cluster.cfg.Sharding.PassivationFrequencySketchWidthMultiplier
		}
		if settings.PassivationFrequencySketchResetMultiplier == 0 && cluster.cfg.Sharding.PassivationFrequencySketchResetMultiplier > 0 {
			settings.PassivationFrequencySketchResetMultiplier = cluster.cfg.Sharding.PassivationFrequencySketchResetMultiplier
		}
		// Round-2 session 13 — retry/backoff (part 1).
		if settings.RetryInterval == 0 && cluster.cfg.Sharding.RetryInterval > 0 {
			settings.RetryInterval = cluster.cfg.Sharding.RetryInterval
		}
		if settings.BufferSize == 0 && cluster.cfg.Sharding.BufferSize > 0 {
			settings.BufferSize = cluster.cfg.Sharding.BufferSize
		}
		if settings.ShardStartTimeout == 0 && cluster.cfg.Sharding.ShardStartTimeout > 0 {
			settings.ShardStartTimeout = cluster.cfg.Sharding.ShardStartTimeout
		}
		if settings.ShardFailureBackoff == 0 && cluster.cfg.Sharding.ShardFailureBackoff > 0 {
			settings.ShardFailureBackoff = cluster.cfg.Sharding.ShardFailureBackoff
		}
		if settings.EntityRestartBackoff == 0 && cluster.cfg.Sharding.EntityRestartBackoff > 0 {
			settings.EntityRestartBackoff = cluster.cfg.Sharding.EntityRestartBackoff
		}
		if settings.CoordinatorFailureBackoff == 0 && cluster.cfg.Sharding.CoordinatorFailureBackoff > 0 {
			settings.CoordinatorFailureBackoff = cluster.cfg.Sharding.CoordinatorFailureBackoff
		}
		// Round-2 session 14 — retry/backoff (part 2).
		if settings.WaitingForStateTimeout == 0 && cluster.cfg.Sharding.WaitingForStateTimeout > 0 {
			settings.WaitingForStateTimeout = cluster.cfg.Sharding.WaitingForStateTimeout
		}
		if settings.UpdatingStateTimeout == 0 && cluster.cfg.Sharding.UpdatingStateTimeout > 0 {
			settings.UpdatingStateTimeout = cluster.cfg.Sharding.UpdatingStateTimeout
		}
		if settings.ShardRegionQueryTimeout == 0 && cluster.cfg.Sharding.ShardRegionQueryTimeout > 0 {
			settings.ShardRegionQueryTimeout = cluster.cfg.Sharding.ShardRegionQueryTimeout
		}
		if settings.EntityRecoveryStrategy == "" && cluster.cfg.Sharding.EntityRecoveryStrategy != "" {
			settings.EntityRecoveryStrategy = cluster.cfg.Sharding.EntityRecoveryStrategy
		}
		if settings.EntityRecoveryConstantRateFrequency == 0 && cluster.cfg.Sharding.EntityRecoveryConstantRateFrequency > 0 {
			settings.EntityRecoveryConstantRateFrequency = cluster.cfg.Sharding.EntityRecoveryConstantRateFrequency
		}
		if settings.EntityRecoveryConstantRateNumberOfEntities == 0 && cluster.cfg.Sharding.EntityRecoveryConstantRateNumberOfEntities > 0 {
			settings.EntityRecoveryConstantRateNumberOfEntities = cluster.cfg.Sharding.EntityRecoveryConstantRateNumberOfEntities
		}
		if settings.CoordinatorWriteMajorityPlus == 0 && cluster.cfg.Sharding.CoordinatorWriteMajorityPlus != 0 {
			settings.CoordinatorWriteMajorityPlus = cluster.cfg.Sharding.CoordinatorWriteMajorityPlus
		}
		if settings.CoordinatorReadMajorityPlus == 0 && cluster.cfg.Sharding.CoordinatorReadMajorityPlus != 0 {
			settings.CoordinatorReadMajorityPlus = cluster.cfg.Sharding.CoordinatorReadMajorityPlus
		}
		// Round-2 session 15 — miscellaneous flags.
		if !settings.VerboseDebugLogging && cluster.cfg.Sharding.VerboseDebugLogging {
			settings.VerboseDebugLogging = true
		}
		if !settings.FailOnInvalidEntityStateTransition && cluster.cfg.Sharding.FailOnInvalidEntityStateTransition {
			settings.FailOnInvalidEntityStateTransition = true
		}
		if settings.IdleEntityCheckInterval == 0 && cluster.cfg.Sharding.IdleEntityCheckInterval > 0 {
			settings.IdleEntityCheckInterval = cluster.cfg.Sharding.IdleEntityCheckInterval
		}
		// Round-2 session 20 — Coordination Lease for Sharding.
		if settings.Lease == nil {
			if l := cluster.resolveShardingLease(typeName); l != nil {
				settings.Lease = l
			}
		}
		if settings.LeaseRetryDelay == 0 && cluster.cfg.Sharding.LeaseRetryInterval > 0 {
			settings.LeaseRetryDelay = cluster.cfg.Sharding.LeaseRetryInterval
		}
		// Round-2 session 34 — eventsourced remember-entities batch cap.
		if settings.EventSourcedMaxUpdatesPerWrite == 0 && cluster.cfg.Sharding.EventSourcedRememberEntitiesStore.MaxUpdatesPerWrite > 0 {
			settings.EventSourcedMaxUpdatesPerWrite = cluster.cfg.Sharding.EventSourcedRememberEntitiesStore.MaxUpdatesPerWrite
		}
	}

	// 1. Start ShardCoordinator as a singleton
	strategy := settings.AllocationStrategy
	if strategy == nil {
		cluster, ok := sys.(*Cluster)
		if ok && cluster.cfg.Sharding.AdaptiveRebalancing.Enabled {
			cfg := cluster.cfg.Sharding.AdaptiveRebalancing
			reader := sharding.NewGossipMetricsReader(
				cluster.mg,
				cfg.CPUWeight,
				cfg.MemoryWeight,
				cfg.MailboxWeight,
			)
			strategy = sharding.NewAdaptiveAllocationStrategy(
				reader,
				cfg.LoadWeight,
				cfg.RebalanceThreshold,
				cfg.MaxSimultaneousRebalance,
			)
		} else {
			// Pekko semantics: when rebalance-absolute-limit > 0, use the
			// 1.0+ two-phase algorithm (LeastShardAllocationStrategyV2);
			// otherwise keep the legacy threshold-based strategy.
			absLimit := 0
			relLimit := 0.0
			if ok {
				absLimit = cluster.cfg.Sharding.LeastShardAllocation.RebalanceAbsoluteLimit
				relLimit = cluster.cfg.Sharding.LeastShardAllocation.RebalanceRelativeLimit
			}
			if absLimit > 0 {
				strategy = sharding.NewLeastShardAllocationStrategyV2(absLimit, relLimit)
			} else {
				// Default LeastShardAllocationStrategy parameters: prefer HOCON-
				// configured values, fall back to gekka legacy defaults (3, 1).
				threshold := 3
				maxSimul := 1
				if ok {
					if v := cluster.cfg.Sharding.LeastShardAllocation.RebalanceThreshold; v > 0 {
						threshold = v
					}
					if v := cluster.cfg.Sharding.LeastShardAllocation.MaxSimultaneousRebalance; v > 0 {
						maxSimul = v
					}
				}
				strategy = sharding.NewLeastShardAllocationStrategy(threshold, maxSimul)
			}
		}
	}

	// Resolve the per-coordinator rebalance interval. Zero leaves the
	// coordinator's built-in 10s default in place.
	var rebalanceInterval time.Duration
	if cluster, ok := sys.(*Cluster); ok {
		rebalanceInterval = cluster.cfg.Sharding.RebalanceInterval
	}

	coordinatorProps := actor.Props{
		New: func() actor.Actor {
			c := sharding.NewShardCoordinator(strategy)
			if rebalanceInterval > 0 {
				c.RebalanceInterval = rebalanceInterval
			}
			sharding.RegisterCoordinator(typeName, c)
			return c
		},
	}

	var coordinatorRef actor.Ref
	cluster, ok := sys.(*Cluster)
	if ok {
		// Spawn coordinator if we are the oldest
		ua := cluster.cm.OldestNode(settings.Role)
		localUA := cluster.cm.GetLocalAddress()

		if ua != nil && localUA != nil && ua.GetAddress().GetHostname() == localUA.GetAddress().GetHostname() &&
			ua.GetAddress().GetPort() == localUA.GetAddress().GetPort() &&
			ua.GetUid() == localUA.GetUid() {
			_, err := sys.ActorOf(coordinatorProps, typeName+"Coordinator")
			if err != nil {
				fmt.Printf("Sharding: failed to spawn coordinator: %v\n", err)
			} else {
				fmt.Printf("Sharding: spawned coordinator on %s:%d\n", localUA.GetAddress().GetHostname(), localUA.GetAddress().GetPort())
			}
		} else {
			fmt.Printf("Sharding: NOT spawning coordinator (ua=%v, localUA=%v)\n", ua, localUA)
		}

		// Resolve the role under which the coordinator-singleton runs.
		// Pekko semantics: coordinator-singleton-role-override = on (default)
		// means sharding's role wins over coordinator-singleton.role; off
		// means coordinator-singleton.role takes effect (when set).
		coordRole := cluster.cfg.Sharding.CoordinatorSingleton.Role
		if cluster.cfg.Sharding.CoordinatorSingletonRoleOverride {
			coordRole = settings.Role
		} else if coordRole == "" {
			coordRole = settings.Role
		}

		// Always use a proxy to reach the coordinator
		proxy := cluster.SingletonProxy("/user/"+typeName+"Coordinator", coordRole).WithSingletonName("")
		ref, err := sys.ActorOf(actor.Props{
			New: func() actor.Actor {
				return sharding.NewShardCoordinatorProxy(proxy)
			},
		}, typeName+"CoordinatorProxy")
		if err != nil {
			return sharding.ClusterEntityRef[Command]{}, err
		}
		coordinatorRef = ref
	}

	// 2. Start ShardRegion
	entityCreator := func(ctx actor.ActorContext, entityId sharding.EntityId) (actor.Ref, error) {
		p := actor.Props{
			New: func() actor.Actor {
				return ptyped.NewPersistentActor(behaviorFactory(entityId))
			},
		}
		return ctx.ActorOf(p, entityId)
	}

	unmarshaler := func(manifest string, data json.RawMessage) (any, error) {
		typ, ok := sys.GetTypeByManifest(manifest)
		if !ok {
			return nil, fmt.Errorf("sharding: no type registered for manifest %q", manifest)
		}

		var ptr reflect.Value
		if typ.Kind() == reflect.Ptr {
			ptr = reflect.New(typ.Elem())
		} else {
			ptr = reflect.New(typ)
		}
		if err := json.Unmarshal(data, ptr.Interface()); err != nil {
			return nil, err
		}
		if typ.Kind() == reflect.Ptr {
			return ptr.Interface(), nil
		}
		return ptr.Elem().Interface(), nil
	}

	shardSettings := sharding.ShardSettings{
		PassivationIdleTimeout:                     settings.PassivationIdleTimeout,
		RememberEntities:                           settings.RememberEntities,
		Journal:                                    settings.Journal,
		DataCenter:                                 settings.DataCenter,
		HandoffTimeout:                             settings.HandoffTimeout,
		NumberOfShards:                             settings.NumberOfShards,
		GuardianName:                               settings.GuardianName,
		PassivationStrategy:                        settings.PassivationStrategy,
		PassivationActiveEntityLimit:               settings.PassivationActiveEntityLimit,
		RetryInterval:                              settings.RetryInterval,
		BufferSize:                                 settings.BufferSize,
		ShardStartTimeout:                          settings.ShardStartTimeout,
		ShardFailureBackoff:                        settings.ShardFailureBackoff,
		EntityRestartBackoff:                       settings.EntityRestartBackoff,
		CoordinatorFailureBackoff:                  settings.CoordinatorFailureBackoff,
		WaitingForStateTimeout:                     settings.WaitingForStateTimeout,
		UpdatingStateTimeout:                       settings.UpdatingStateTimeout,
		ShardRegionQueryTimeout:                    settings.ShardRegionQueryTimeout,
		EntityRecoveryStrategy:                     settings.EntityRecoveryStrategy,
		EntityRecoveryConstantRateFrequency:        settings.EntityRecoveryConstantRateFrequency,
		EntityRecoveryConstantRateNumberOfEntities: settings.EntityRecoveryConstantRateNumberOfEntities,
		CoordinatorWriteMajorityPlus:               settings.CoordinatorWriteMajorityPlus,
		CoordinatorReadMajorityPlus:                settings.CoordinatorReadMajorityPlus,
		VerboseDebugLogging:                        settings.VerboseDebugLogging,
		FailOnInvalidEntityStateTransition:         settings.FailOnInvalidEntityStateTransition,
		IdleEntityCheckInterval:                    settings.IdleEntityCheckInterval,
		Lease:                                      settings.Lease,
		LeaseRetryDelay:                            settings.LeaseRetryDelay,
		PassivationWindowProportion:                settings.PassivationWindowProportion,
		PassivationWindowPolicy:                    settings.PassivationWindowPolicy,
		PassivationFilter:                          settings.PassivationFilter,
		PassivationFrequencySketchDepth:            settings.PassivationFrequencySketchDepth,
		PassivationFrequencySketchCounterBits:      settings.PassivationFrequencySketchCounterBits,
		PassivationFrequencySketchWidthMultiplier:  settings.PassivationFrequencySketchWidthMultiplier,
		PassivationFrequencySketchResetMultiplier:  settings.PassivationFrequencySketchResetMultiplier,
		EventSourcedMaxUpdatesPerWrite:             settings.EventSourcedMaxUpdatesPerWrite,
	}

	// Populate IsLocalDC when both the cluster and DataCenter are available.
	if cluster != nil && settings.DataCenter != "" {
		dc := settings.DataCenter
		shardSettings.IsLocalDC = func(host string, port uint32) bool {
			return cluster.cm.IsInDataCenter(host, port, dc)
		}
	}

	// Wire a DData-backed ShardStore when the operator has selected the
	// "ddata" remember-entities backend. When RememberEntities is false the
	// Store is still plumbed through but will be ignored by the Shard.
	if cluster != nil && settings.RememberEntitiesStore == "ddata" && cluster.repl != nil {
		shardSettings.Store = sharding.NewDDataEntityStore(cluster.repl, typeName)
	}

	// Wire a journal-backed ShardStore when the operator has selected the
	// "eventsourced" remember-entities backend (Round-2 session 35).  The
	// store coalesces EntityStarted/EntityStopped events using the same
	// max-updates-per-write cap that drives the legacy in-Shard batching, so
	// either path produces equivalent batched journal writes.
	//
	// Selection precedence: an explicitly-supplied settings.Store always
	// wins; otherwise we fall back to settings.Journal, then to the
	// cluster's provisioned Journal, then to a fresh InMemoryJournal so unit
	// tests don't have to wire one up.
	if shardSettings.Store == nil && settings.RememberEntitiesStore == "eventsourced" {
		journal := settings.Journal
		if journal == nil && cluster != nil {
			journal = cluster.Journal()
		}
		if journal == nil {
			journal = persistence.NewInMemoryJournal()
		}
		shardSettings.Journal = journal
		shardSettings.Store = sharding.NewEventSourcedEntityStore(journal, typeName, shardSettings.EventSourcedMaxUpdatesPerWrite)
	}

	region, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor {
			return sharding.NewShardRegion(typeName, entityCreator, unmarshaler, extract, coordinatorRef, shardSettings)
		},
	}, typeName+"Region")

	if err != nil {
		return sharding.ClusterEntityRef[Command]{}, fmt.Errorf("failed to spawn region: %w", err)
	}

	return sharding.ClusterEntityRef[Command]{
		EntityId: "", // entry point for the region
		Region:   region,
	}, nil
}

// EntityRefFor returns a type-safe EntityRef for a specific entity.
func EntityRefFor[M any](sys ActorSystem, typeName string, entityID string) (*styped.EntityRef[M], error) {
	regionPath := "/user/" + typeName + "Region"
	ref, err := sys.ActorSelection(regionPath).Resolve(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("sharding: failed to resolve region %q: %w", regionPath, err)
	}
	return styped.NewEntityRef[M](typeName, entityID, ref), nil
}

// StartTyped starts cluster sharding for a given typed entity.
// It returns the ShardRegion actor reference.
func StartTyped[M any, Event any, State any](
	sys ActorSystem,
	typeName string,
	behaviorFactory func(entityId string) *ptyped.EventSourcedBehavior[M, Event, State],
	extract sharding.ExtractEntityId,
	settings ShardingSettings,
) (actor.Ref, error) {
	res, err := StartSharding(sys, typeName, behaviorFactory, extract, settings)
	if err != nil {
		return nil, err
	}
	return res.Region, nil
}

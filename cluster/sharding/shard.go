/*
 * shard.go
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
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// shardLeaseRetryDefault is the default backoff between Shard lease-acquisition
// retries when ShardSettings.LeaseRetryDelay is unset.  Mirrors Pekko's
// pekko.cluster.sharding.lease-retry-interval default.
const shardLeaseRetryDefault = 5 * time.Second

// ── Internal event types written to the journal when RememberEntities is on ──

// entityStartedEvent is persisted when an entity is first spawned.
type entityStartedEvent struct {
	EntityId EntityId `json:"entityId"`
}

// entityStoppedEvent is persisted when an entity is removed from memory.
type entityStoppedEvent struct {
	EntityId EntityId `json:"entityId"`
}

// checkPassivationMsg is a periodic self-message that triggers the idle scan.
type checkPassivationMsg struct{}

// entityRecoveryTickMsg is a self-message used by the constant-rate entity
// recovery strategy: each tick spawns the next batch of remembered entities.
type entityRecoveryTickMsg struct{}

// ── Shard ─────────────────────────────────────────────────────────────────────

// Shard manages entities within a shard.
type Shard struct {
	actor.BaseActor
	typeName           string
	shardId            string
	entityCreator      func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error)
	messageUnmarshaler func(manifest string, data json.RawMessage) (any, error)
	entities           map[EntityId]actor.Ref
	settings           ShardSettings

	// passivation: tracks the last time each entity received a message.
	lastActivity map[EntityId]time.Time

	// remember-entities (journal path): monotonic sequence number.
	seqNr         uint64
	persistenceId string
	journal       persistence.Journal

	// remember-entities (store path): set-based; passivation keeps entities.
	store ShardStore

	// pendingRecovery is the queue of remembered entity IDs that have not
	// yet been re-spawned. Populated by recoverFromStore /
	// recoverFromJournal when EntityRecoveryStrategy = "constant"; drained
	// in batches via entityRecoveryTickMsg.
	pendingRecovery []EntityId

	// handoff stash: when inHandoff is true, incoming ShardingEnvelope
	// messages are buffered here rather than delivered to entities.  On
	// ShardDrainRequest the buffer is forwarded back to the region so that
	// the messages can be re-routed to the new shard home.
	inHandoff    bool
	handoffStash []ShardingEnvelope

	// leaseHeld tracks whether settings.Lease (if any) is currently acquired
	// by this Shard.  Set on Acquire and cleared on Release / loss callback.
	leaseHeld bool
}

// NewShard creates a Shard for the given type/shard identifiers.
func NewShard(
	typeName string,
	shardId string,
	creator func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error),
	unmarshaler func(string, json.RawMessage) (any, error),
	settings ShardSettings,
) *Shard {
	return &Shard{
		BaseActor:          actor.NewBaseActor(),
		typeName:           typeName,
		shardId:            shardId,
		entityCreator:      creator,
		messageUnmarshaler: unmarshaler,
		entities:           make(map[EntityId]actor.Ref),
		settings:           settings,
		lastActivity:       make(map[EntityId]time.Time),
	}
}

// PreStart recovers entity membership from the configured store (when
// RememberEntities is on) and starts the passivation check timer.
//
// Two recovery paths exist:
//   - Store path (ShardSettings.Store or auto-created FileStore from
//     JournalStorePath): set-based; all previously recorded entities are
//     re-spawned unconditionally, including those that were passivated.
//   - Journal path (ShardSettings.Journal): event-sourced; only entities
//     that were active (not passivated) at the time of shutdown are recovered.
func (s *Shard) PreStart() {
	// ── Coordination Lease ────────────────────────────────────────────────
	// When ShardSettings.Lease is configured, block on Acquire before
	// becoming active so two replicas of the same shard never run at once
	// (e.g. across an SBR-induced split).
	s.acquireLease()

	// ── Remember Entities ─────────────────────────────────────────────────
	if s.settings.RememberEntities {
		switch {
		case s.settings.Store != nil:
			// Explicit store supplied — use set-based recovery.
			s.store = s.settings.Store
			s.recoverFromStore()

		case s.settings.JournalStorePath != "":
			// Auto-create a FileStore from the configured path.
			fs, err := NewFileStore(s.settings.JournalStorePath)
			if err != nil {
				s.Log().Error("remember-entities: failed to create FileStore",
					"path", s.settings.JournalStorePath, "error", err)
			} else {
				s.store = fs
				s.recoverFromStore()
			}

		default:
			// Fall back to event-sourced journal path.
			s.journal = s.settings.Journal
			if s.journal == nil {
				s.journal = persistence.NewInMemoryJournal()
			}
			s.persistenceId = "shard-" + s.typeName + "-" + s.shardId
			s.recoverFromJournal()
		}
	}

	// ── Passivation: schedule first idle check ────────────────────────────
	if s.settings.PassivationIdleTimeout > 0 {
		s.schedulePassivationCheck()
	}
}

// recoverFromJournal replays EntityStarted/EntityStopped events to rebuild
// the active entity set and re-spawns surviving entities.
func (s *Shard) recoverFromJournal() {
	ctx := context.Background()

	// Read highest sequence number so we know where we are.
	highest, err := s.journal.ReadHighestSequenceNr(ctx, s.persistenceId, 0)
	if err != nil {
		s.Log().Error("remember-entities: failed to read highest seqNr", "error", err)
		return
	}
	if highest == 0 {
		return // no events — nothing to recover
	}

	// Replay events to determine which entities were active at shutdown.
	active := make(map[EntityId]struct{})
	err = s.journal.ReplayMessages(ctx, s.persistenceId, 1, highest, 0, func(repr persistence.PersistentRepr) {
		switch e := repr.Payload.(type) {
		case entityStartedEvent:
			active[e.EntityId] = struct{}{}
		case entityStoppedEvent:
			delete(active, e.EntityId)
		}
	})
	if err != nil {
		s.Log().Error("remember-entities: replay failed", "error", err)
		return
	}

	s.seqNr = highest

	ids := make([]EntityId, 0, len(active))
	for entityId := range active {
		ids = append(ids, entityId)
	}
	s.applyEntityRecoveryStrategy(ids, "journal")
}

// recoverFromStore replays the ShardStore's entity set and re-spawns every
// entity it contains.  Unlike recoverFromJournal, passivated entities are NOT
// filtered out — the store is a live set that only shrinks on explicit
// termination.
func (s *Shard) recoverFromStore() {
	ids, err := s.store.GetEntities(s.shardId)
	if err != nil {
		s.Log().Error("remember-entities: failed to read entities from store",
			"shardId", s.shardId, "error", err)
		return
	}
	s.applyEntityRecoveryStrategy(ids, "store")
}

// applyEntityRecoveryStrategy spawns the recovered entity set according to
// the configured EntityRecoveryStrategy. The default ("all") spawns every
// entity inline; "constant" enqueues the slice for batched recovery driven by
// entityRecoveryTickMsg.
func (s *Shard) applyEntityRecoveryStrategy(ids []EntityId, source string) {
	if len(ids) == 0 {
		return
	}
	if s.settings.EntityRecoveryStrategy != EntityRecoveryStrategyConstant {
		// Default "all" strategy: spawn every entity at once.
		for _, entityId := range ids {
			s.spawnRecoveredEntity(entityId, source)
		}
		return
	}

	// Constant-rate strategy: spawn the first batch immediately, then drain
	// the rest one batch per entityRecoveryTickMsg tick.
	batchSize := s.settings.EntityRecoveryConstantRateNumberOfEntities
	if batchSize <= 0 {
		batchSize = 5
	}

	first := batchSize
	if first > len(ids) {
		first = len(ids)
	}
	for _, entityId := range ids[:first] {
		s.spawnRecoveredEntity(entityId, source)
	}
	if first < len(ids) {
		s.pendingRecovery = append(s.pendingRecovery, ids[first:]...)
		s.scheduleEntityRecoveryTick()
	}
}

// spawnRecoveredEntity creates one previously-remembered entity and registers
// it with the local Shard. Failures are logged and do not abort recovery of
// the remaining entities.
func (s *Shard) spawnRecoveredEntity(entityId EntityId, source string) {
	entity, spawnErr := s.entityCreator(s.System(), entityId)
	if spawnErr != nil {
		s.Log().Error("remember-entities: failed to re-spawn entity",
			"source", source, "entityId", entityId, "error", spawnErr)
		return
	}
	s.entities[entityId] = entity
	if s.settings.PassivationIdleTimeout > 0 {
		s.lastActivity[entityId] = time.Now()
	}
	s.Log().Info("remember-entities: recovered entity",
		"source", source, "entityId", entityId, "shardId", s.shardId)
}

// scheduleEntityRecoveryTick arms a one-shot timer that delivers
// entityRecoveryTickMsg after the configured frequency. The Receive handler
// re-arms it while pendingRecovery still has entries.
func (s *Shard) scheduleEntityRecoveryTick() {
	freq := s.settings.EntityRecoveryConstantRateFrequency
	if freq <= 0 {
		freq = 100 * time.Millisecond
	}
	self := s.Self()
	if self == nil {
		return
	}
	time.AfterFunc(freq, func() { self.Tell(entityRecoveryTickMsg{}) })
}

// drainEntityRecoveryBatch spawns the next batch of pending recovered
// entities (up to EntityRecoveryConstantRateNumberOfEntities) and re-arms the
// tick when more remain.
func (s *Shard) drainEntityRecoveryBatch() {
	if len(s.pendingRecovery) == 0 {
		return
	}
	batchSize := s.settings.EntityRecoveryConstantRateNumberOfEntities
	if batchSize <= 0 {
		batchSize = 5
	}
	if batchSize > len(s.pendingRecovery) {
		batchSize = len(s.pendingRecovery)
	}
	batch := s.pendingRecovery[:batchSize]
	s.pendingRecovery = s.pendingRecovery[batchSize:]
	for _, entityId := range batch {
		s.spawnRecoveredEntity(entityId, "constant")
	}
	if len(s.pendingRecovery) > 0 {
		s.scheduleEntityRecoveryTick()
	}
}

// invalidStateTransition reports an invalid Shard state-machine transition.
// When FailOnInvalidEntityStateTransition is true (Pekko-test parity) the
// shard panics so the violation surfaces immediately; otherwise the event
// is logged at WARN level and the Shard tries to continue.
func (s *Shard) invalidStateTransition(reason string) {
	if s.settings.FailOnInvalidEntityStateTransition {
		panic(fmt.Sprintf("sharding: invalid entity state transition (shardId=%s): %s",
			s.shardId, reason))
	}
	s.Log().Warn("invalid entity state transition",
		"shardId", s.shardId, "reason", reason)
}

// vdebug emits a Log().Debug() line only when VerboseDebugLogging is on.
// Used for the per-message debug lines that should not appear in production
// log output by default.
func (s *Shard) vdebug(msg string, args ...any) {
	if !s.settings.VerboseDebugLogging {
		return
	}
	s.Log().Debug(msg, args...)
}

// schedulePassivationCheck sends a checkPassivationMsg to self after one
// check interval. When IdleEntityCheckInterval is set, that value is used
// directly; otherwise the cadence falls back to PassivationIdleTimeout / 2
// (Pekko's "default" semantics for
// passivation.default-idle-strategy.idle-entity.interval), with a minimum
// of 500 ms in either case.
func (s *Shard) schedulePassivationCheck() {
	var interval time.Duration
	if s.settings.IdleEntityCheckInterval > 0 {
		interval = s.settings.IdleEntityCheckInterval
	} else {
		interval = s.settings.PassivationIdleTimeout / 2
	}
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	self := s.Self()
	time.AfterFunc(interval, func() {
		self.Tell(checkPassivationMsg{})
	})
}

// Receive handles messages for the shard.
func (s *Shard) Receive(msg any) {
	switch m := msg.(type) {
	case ShardingEnvelope:
		s.handleEnvelope(m)

	case ShardBeginHandoff:
		// Region is being handed off: buffer subsequent messages.
		// fail-on-invalid-entity-state-transition: entering handoff
		// twice (without an intervening drain) is an invalid transition.
		if s.inHandoff {
			s.invalidStateTransition("ShardBeginHandoff while already in handoff")
		}
		s.inHandoff = true
		s.Log().Info("Shard entering handoff mode", "shardId", s.shardId)

	case ShardDrainRequest:
		// Drain is only valid while we are in handoff. Otherwise the
		// region's drain protocol has gone out of sync.
		if !s.inHandoff {
			s.invalidStateTransition("ShardDrainRequest without prior ShardBeginHandoff")
		}
		// Flush stash back to region so it can re-route messages to the new home.
		for _, env := range s.handoffStash {
			m.RegionRef.Tell(env)
		}
		stashLen := len(s.handoffStash)
		s.handoffStash = nil
		s.inHandoff = false
		s.Log().Info("Shard drained stash to region",
			"shardId", s.shardId, "count", stashLen)
		m.RegionRef.Tell(ShardDrainResponse{ShardId: s.shardId})

	case actor.Passivate:
		// Passivation: remove from in-memory map but do NOT call
		// store.RemoveEntity — the entity remains remembered so it will be
		// re-spawned on the next shard restart.
		s.handlePassivate(m.Entity)

	case actor.TerminatedMessage:
		// Explicit entity termination: remove from both memory and the store
		// so it is not re-spawned after a restart.
		s.handleTerminated(m.TerminatedActor())

	case checkPassivationMsg:
		s.checkIdleEntities()
		// Re-arm the timer.
		s.schedulePassivationCheck()

	case entityRecoveryTickMsg:
		// Constant-rate recovery: spawn the next batch and re-arm if more
		// entities remain.
		s.drainEntityRecoveryBatch()
	}
}

// handleEnvelope routes a ShardingEnvelope to its entity, spawning if needed.
func (s *Shard) handleEnvelope(m ShardingEnvelope) {
	// During handoff, buffer messages for later replay rather than delivering.
	if s.inHandoff {
		s.handoffStash = append(s.handoffStash, m)
		s.vdebug("Shard stashing message during handoff",
			"shardId", s.shardId, "entityId", m.EntityId)
		return
	}
	s.vdebug("Shard delivering envelope",
		"shardId", s.shardId, "entityId", m.EntityId)

	// Unmarshal the user message.
	var userMsg any = m.Message
	if s.messageUnmarshaler != nil {
		var err error
		userMsg, err = s.messageUnmarshaler(m.MessageManifest, m.Message)
		if err != nil {
			s.Log().Error("failed to unmarshal user message",
				"manifest", m.MessageManifest, "error", err)
			return
		}
		s.Log().Debug("unmarshaled user message", "type", fmt.Sprintf("%T", userMsg))
	}

	entity, ok := s.entities[m.EntityId]
	if !ok {
		var err error
		entity, err = s.entityCreator(s.System(), m.EntityId)
		if err != nil {
			s.Log().Error("failed to spawn entity", "entityId", m.EntityId, "error", err)
			return
		}
		s.entities[m.EntityId] = entity
		// Store path: record new entity in the set-based store.
		if s.store != nil {
			if addErr := s.store.AddEntity(s.shardId, m.EntityId); addErr != nil {
				s.Log().Error("remember-entities: store.AddEntity failed",
					"entityId", m.EntityId, "error", addErr)
			}
		}
		// Journal path: persist EntityStarted event.
		s.persistEntityStarted(m.EntityId)

		// LRU eviction: if the active strategy is LRU, check entity limit.
		// Round-2 session 24 normalises the Pekko-canonical name
		// "least-recently-used" with the gekka legacy alias
		// "custom-lru-strategy"; both route to the same eviction loop.
		if isLRUStrategy(s.settings.PassivationStrategy) {
			s.checkLRUEviction()
		}
	}

	// Update activity timestamp for passivation tracking.
	if s.settings.PassivationIdleTimeout > 0 {
		s.lastActivity[m.EntityId] = time.Now()
	}

	entity.Tell(userMsg, s.Sender())
}

// handlePassivate processes a passivation request from an entity (self-initiated
// or shard-initiated via idle timeout).
func (s *Shard) handlePassivate(entity actor.Ref) {
	for id, ref := range s.entities {
		if ref.Path() == entity.Path() {
			if stopper, ok := s.System().(interface{ Stop(actor.Ref) }); ok {
				stopper.Stop(entity)
			}
			delete(s.entities, id)
			delete(s.lastActivity, id)
			s.persistEntityStopped(id)
			s.Log().Info("entity passivated", "entityId", id)
			return
		}
	}
}

// handleTerminated processes an actor.TerminatedMessage for an entity that was
// explicitly stopped (e.g., by the actor system or a supervising parent).
// Unlike passivation, this permanently removes the entity from the ShardStore
// so it is not re-spawned after a shard restart.
func (s *Shard) handleTerminated(terminated actor.Ref) {
	for id, ref := range s.entities {
		if ref.Path() == terminated.Path() {
			delete(s.entities, id)
			delete(s.lastActivity, id)
			if s.store != nil {
				if err := s.store.RemoveEntity(s.shardId, id); err != nil {
					s.Log().Error("remember-entities: store.RemoveEntity failed",
						"entityId", id, "error", err)
				}
			}
			s.persistEntityStopped(id)
			s.Log().Info("entity terminated (removed from store)", "entityId", id)
			return
		}
	}
}

// checkLRUEviction evicts the least-recently-used entity when the active
// entity count exceeds the configured limit (custom-lru-strategy).
func (s *Shard) checkLRUEviction() {
	limit := s.settings.PassivationActiveEntityLimit
	if limit <= 0 {
		limit = 100000
	}
	if len(s.entities) <= limit {
		return
	}
	var oldestID EntityId
	var oldestTime time.Time
	for id, t := range s.lastActivity {
		if oldestTime.IsZero() || t.Before(oldestTime) {
			oldestID = id
			oldestTime = t
		}
	}
	if oldestID != "" {
		if ref, ok := s.entities[oldestID]; ok {
			s.Log().Info("LRU evicting entity", "entityId", oldestID, "activeEntities", len(s.entities), "limit", limit)
			s.handlePassivate(ref)
		}
	}
}

// checkIdleEntities scans all entities for idle timeout and passivates them.
func (s *Shard) checkIdleEntities() {
	if s.settings.PassivationIdleTimeout <= 0 {
		return
	}
	now := time.Now()
	for id, last := range s.lastActivity {
		if now.Sub(last) >= s.settings.PassivationIdleTimeout {
			s.Log().Info("passivating idle entity",
				"entityId", id, "idleFor", now.Sub(last))
			if ref, ok := s.entities[id]; ok {
				s.handlePassivate(ref)
			}
		}
	}
}

// persistEntityStarted writes an EntityStarted event when RememberEntities is on.
func (s *Shard) persistEntityStarted(entityId EntityId) {
	if !s.settings.RememberEntities || s.journal == nil {
		return
	}
	s.seqNr++
	_ = s.journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{
		{
			PersistenceID: s.persistenceId,
			SequenceNr:    s.seqNr,
			Payload:       entityStartedEvent{EntityId: entityId},
		},
	})
}

// persistEntityStopped writes an EntityStopped event when RememberEntities is on.
func (s *Shard) persistEntityStopped(entityId EntityId) {
	if !s.settings.RememberEntities || s.journal == nil {
		return
	}
	s.seqNr++
	_ = s.journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{
		{
			PersistenceID: s.persistenceId,
			SequenceNr:    s.seqNr,
			Payload:       entityStoppedEvent{EntityId: entityId},
		},
	})
}

// PostStop releases the coordination lease (if held) so a peer Shard can
// take over once handoff completes.
func (s *Shard) PostStop() {
	s.releaseLease()
}

// acquireLease blocks on settings.Lease.Acquire until it succeeds.  When
// Acquire returns false or errors, the call sleeps for LeaseRetryDelay and
// retries — mirroring Pekko's Shard.acquireLease loop.  Returns immediately
// when no lease is configured.
func (s *Shard) acquireLease() {
	l := s.settings.Lease
	if l == nil {
		return
	}
	delay := s.settings.LeaseRetryDelay
	if delay <= 0 {
		delay = shardLeaseRetryDefault
	}
	ctx := context.Background()
	for {
		ok, err := l.Acquire(ctx, func(loseErr error) {
			s.Log().Warn("shard lease lost", "shard", s.shardId, "error", loseErr)
			s.leaseHeld = false
		})
		if err != nil {
			s.Log().Error("shard lease acquire failed", "shard", s.shardId, "error", err)
		}
		if ok {
			s.leaseHeld = true
			return
		}
		s.Log().Info("shard lease acquire retrying", "shard", s.shardId, "delay", delay)
		time.Sleep(delay)
	}
}

// releaseLease releases settings.Lease when this Shard currently holds it.
func (s *Shard) releaseLease() {
	if s.settings.Lease == nil || !s.leaseHeld {
		return
	}
	if _, err := s.settings.Lease.Release(context.Background()); err != nil {
		s.Log().Error("shard lease release failed", "shard", s.shardId, "error", err)
	}
	s.leaseHeld = false
}

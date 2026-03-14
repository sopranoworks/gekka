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

	// remember-entities: monotonic sequence number for journal writes.
	seqNr         uint64
	persistenceId string
	journal       persistence.Journal
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

// PreStart recovers entity membership from the journal (when RememberEntities
// is on) and starts the passivation check timer.
func (s *Shard) PreStart() {
	// ── Remember Entities: setup journal ─────────────────────────────────
	if s.settings.RememberEntities {
		s.journal = s.settings.Journal
		if s.journal == nil {
			s.journal = persistence.NewInMemoryJournal()
		}
		s.persistenceId = "shard-" + s.typeName + "-" + s.shardId

		// Recover the set of active entities by replaying all events.
		s.recoverFromJournal()
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

	// Re-spawn each previously active entity.
	for entityId := range active {
		entity, spawnErr := s.entityCreator(s.System(), entityId)
		if spawnErr != nil {
			s.Log().Error("remember-entities: failed to re-spawn entity",
				"entityId", entityId, "error", spawnErr)
			continue
		}
		s.entities[entityId] = entity
		if s.settings.PassivationIdleTimeout > 0 {
			s.lastActivity[entityId] = time.Now()
		}
		s.Log().Info("remember-entities: recovered entity", "entityId", entityId,
			"persistenceId", s.persistenceId)
	}
}

// schedulePassivationCheck sends a checkPassivationMsg to self after one
// check interval.  The check interval is PassivationIdleTimeout/4, minimum
// 500 ms.
func (s *Shard) schedulePassivationCheck() {
	interval := max(s.settings.PassivationIdleTimeout/4, 500*time.Millisecond)
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

	case actor.Passivate:
		s.handlePassivate(m.Entity)

	case checkPassivationMsg:
		s.checkIdleEntities()
		// Re-arm the timer.
		s.schedulePassivationCheck()
	}
}

// handleEnvelope routes a ShardingEnvelope to its entity, spawning if needed.
func (s *Shard) handleEnvelope(m ShardingEnvelope) {
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
		s.persistEntityStarted(m.EntityId)
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


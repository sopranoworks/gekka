/*
 * eventsourced_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"fmt"
	"sync"

	"github.com/sopranoworks/gekka/persistence"
)

// EventSourcedEntityStore is a ShardStore backed by a persistence.Journal.
//
// Each shard's entity set is materialized from a journal log of
// entityStartedEvent / entityStoppedEvent records keyed by persistenceId
// "shard-<typeName>-<shardId>".  GetEntities returns the active set
// (Started \ Stopped) so passivated entities are remembered until they are
// explicitly terminated — matching Pekko's EventSourcedRememberEntitiesStore
// semantics.
//
// Use this when pekko.cluster.sharding.remember-entities-store = "eventsourced".
//
// The store coalesces consecutive AddEntity / RemoveEntity events into a
// single journal write when MaxUpdatesPerWrite > 0; partial buffers are
// flushed by Flush, which Shard.PostStop invokes.
type EventSourcedEntityStore struct {
	journal            persistence.Journal
	typeName           string
	maxUpdatesPerWrite int

	mu     sync.Mutex
	shards map[ShardId]*esShardState
}

// esShardState holds the per-shard cache and pending-write buffer.
type esShardState struct {
	persistenceId string
	seqNr         uint64
	active        map[EntityId]struct{}
	pending       []persistence.PersistentRepr
	initialized   bool
}

// NewEventSourcedEntityStore returns a journal-backed store that namespaces
// each shard's entity set by typeName.  journal must not be nil.
//
// maxUpdatesPerWrite, when > 0, coalesces AddEntity / RemoveEntity events
// into a single AsyncWriteMessages call once the per-shard buffer hits the
// cap.  A value of 0 writes one event per call (legacy behaviour).
func NewEventSourcedEntityStore(journal persistence.Journal, typeName string, maxUpdatesPerWrite int) *EventSourcedEntityStore {
	return &EventSourcedEntityStore{
		journal:            journal,
		typeName:           typeName,
		maxUpdatesPerWrite: maxUpdatesPerWrite,
		shards:             make(map[ShardId]*esShardState),
	}
}

// shardStateLocked returns the per-shard cache, allocating it lazily.
// Caller must hold s.mu.
func (s *EventSourcedEntityStore) shardStateLocked(shardID ShardId) *esShardState {
	if st, ok := s.shards[shardID]; ok {
		return st
	}
	st := &esShardState{
		persistenceId: fmt.Sprintf("shard-%s-%s", s.typeName, shardID),
		active:        make(map[EntityId]struct{}),
	}
	s.shards[shardID] = st
	return st
}

// initLocked replays the journal once per shard to materialise st.active.
// Subsequent calls are no-ops.  Caller must hold s.mu.
func (s *EventSourcedEntityStore) initLocked(st *esShardState) error {
	if st.initialized {
		return nil
	}
	ctx := context.Background()
	highest, err := s.journal.ReadHighestSequenceNr(ctx, st.persistenceId, 0)
	if err != nil {
		return err
	}
	if highest > 0 {
		err = s.journal.ReplayMessages(ctx, st.persistenceId, 1, highest, 0, func(repr persistence.PersistentRepr) {
			switch e := repr.Payload.(type) {
			case entityStartedEvent:
				st.active[e.EntityId] = struct{}{}
			case entityStoppedEvent:
				delete(st.active, e.EntityId)
			}
		})
		if err != nil {
			return err
		}
	}
	st.seqNr = highest
	st.initialized = true
	return nil
}

// AddEntity records entityID as active in shardID.  Idempotent.
func (s *EventSourcedEntityStore) AddEntity(shardID ShardId, entityID EntityId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.shardStateLocked(shardID)
	if err := s.initLocked(st); err != nil {
		return err
	}
	if _, ok := st.active[entityID]; ok {
		return nil
	}
	st.active[entityID] = struct{}{}
	st.seqNr++
	return s.appendOrFlushLocked(st, persistence.PersistentRepr{
		PersistenceID: st.persistenceId,
		SequenceNr:    st.seqNr,
		Payload:       entityStartedEvent{EntityId: entityID},
	})
}

// RemoveEntity removes entityID from shardID's set.  Called only on explicit
// entity termination — passivation must NOT call this so that passivated
// entities are recovered after a Shard restart.
func (s *EventSourcedEntityStore) RemoveEntity(shardID ShardId, entityID EntityId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.shardStateLocked(shardID)
	if err := s.initLocked(st); err != nil {
		return err
	}
	if _, ok := st.active[entityID]; !ok {
		return nil
	}
	delete(st.active, entityID)
	st.seqNr++
	return s.appendOrFlushLocked(st, persistence.PersistentRepr{
		PersistenceID: st.persistenceId,
		SequenceNr:    st.seqNr,
		Payload:       entityStoppedEvent{EntityId: entityID},
	})
}

// GetEntities returns the current set of active entity IDs for shardID.
// Replays the journal on first call per shard; subsequent calls hit the
// in-memory cache.
func (s *EventSourcedEntityStore) GetEntities(shardID ShardId) ([]EntityId, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.shardStateLocked(shardID)
	if err := s.initLocked(st); err != nil {
		return nil, err
	}
	out := make([]EntityId, 0, len(st.active))
	for id := range st.active {
		out = append(out, id)
	}
	return out, nil
}

// Flush writes any buffered events for shardID as a single batch.  Safe to
// call when the buffer is empty.  Called by Shard.PostStop via the
// FlushableStore extension.
func (s *EventSourcedEntityStore) Flush(shardID ShardId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.shards[shardID]
	if !ok {
		return nil
	}
	return s.flushLocked(st)
}

// FlushAll flushes pending events for every shard the store has touched.
func (s *EventSourcedEntityStore) FlushAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.shards {
		if err := s.flushLocked(st); err != nil {
			return err
		}
	}
	return nil
}

// appendOrFlushLocked queues repr in the per-shard buffer, flushing
// immediately when batching is disabled or the buffer hits the cap.
// Caller must hold s.mu.
func (s *EventSourcedEntityStore) appendOrFlushLocked(st *esShardState, repr persistence.PersistentRepr) error {
	if s.maxUpdatesPerWrite <= 0 {
		return s.journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{repr})
	}
	st.pending = append(st.pending, repr)
	if len(st.pending) >= s.maxUpdatesPerWrite {
		return s.flushLocked(st)
	}
	return nil
}

// flushLocked writes any buffered events to the journal in one batch.
// Caller must hold s.mu.
func (s *EventSourcedEntityStore) flushLocked(st *esShardState) error {
	if len(st.pending) == 0 {
		return nil
	}
	batch := st.pending
	st.pending = nil
	return s.journal.AsyncWriteMessages(context.Background(), batch)
}

// FlushableStore is an optional ShardStore extension.  Stores that batch
// writes implement Flush, which the Shard calls during PostStop so any
// buffered events reach the journal before the Shard goroutine exits.
type FlushableStore interface {
	Flush(shardID ShardId) error
}

// Compile-time guards.
var (
	_ ShardStore     = (*EventSourcedEntityStore)(nil)
	_ FlushableStore = (*EventSourcedEntityStore)(nil)
)

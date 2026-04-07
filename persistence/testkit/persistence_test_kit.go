/*
 * persistence_test_kit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package testkit provides an in-memory persistence backend with programmable
// failure injection for unit-testing persistent actors.
//
// PersistenceTestKit wraps an in-memory journal and snapshot store and adds the
// ability to make the next persist or read call fail with a given error, inspect
// all persisted events, and assert specific events were persisted.
//
// Usage:
//
//	kit := testkit.NewPersistenceTestKit()
//	// use kit.Journal() and kit.SnapshotStore() as persistence backends
//	kit.FailNextPersist("my-actor", errors.New("disk full"))
//	// next AsyncWriteMessages for "my-actor" returns the injected error
package testkit

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/sopranoworks/gekka/persistence"
)

// PersistenceTestKit provides an in-memory journal and snapshot store with
// programmable failure injection for testing persistent actors.
type PersistenceTestKit struct {
	mu sync.Mutex

	// underlying stores
	journal  *testJournal
	snapshot *testSnapshotStore
}

// NewPersistenceTestKit creates a new test kit with empty in-memory stores.
func NewPersistenceTestKit() *PersistenceTestKit {
	return &PersistenceTestKit{
		journal:  newTestJournal(),
		snapshot: newTestSnapshotStore(),
	}
}

// Journal returns the test journal as a persistence.Journal.
func (k *PersistenceTestKit) Journal() persistence.Journal {
	return k.journal
}

// SnapshotStore returns the test snapshot store as a persistence.SnapshotStore.
func (k *PersistenceTestKit) SnapshotStore() persistence.SnapshotStore {
	return k.snapshot
}

// FailNextPersist makes the next AsyncWriteMessages call for the given
// persistenceId fail with err. The failure is consumed on first use.
func (k *PersistenceTestKit) FailNextPersist(persistenceId string, err error) {
	k.journal.mu.Lock()
	defer k.journal.mu.Unlock()
	k.journal.persistFailures[persistenceId] = err
}

// FailNextRead makes the next ReplayMessages call for the given persistenceId
// fail with err. The failure is consumed on first use.
func (k *PersistenceTestKit) FailNextRead(persistenceId string, err error) {
	k.journal.mu.Lock()
	defer k.journal.mu.Unlock()
	k.journal.readFailures[persistenceId] = err
}

// FailNextSnapshot makes the next SaveSnapshot call for the given
// persistenceId fail with err. The failure is consumed on first use.
func (k *PersistenceTestKit) FailNextSnapshot(persistenceId string, err error) {
	k.snapshot.mu.Lock()
	defer k.snapshot.mu.Unlock()
	k.snapshot.saveFailures[persistenceId] = err
}

// FailNextLoadSnapshot makes the next LoadSnapshot call for the given
// persistenceId fail with err. The failure is consumed on first use.
func (k *PersistenceTestKit) FailNextLoadSnapshot(persistenceId string, err error) {
	k.snapshot.mu.Lock()
	defer k.snapshot.mu.Unlock()
	k.snapshot.loadFailures[persistenceId] = err
}

// ExpectNextPersisted asserts that the last event persisted for persistenceId
// matches the expected value. Returns an error if the assertion fails.
func (k *PersistenceTestKit) ExpectNextPersisted(persistenceId string, expected any) error {
	k.journal.mu.Lock()
	defer k.journal.mu.Unlock()

	events := k.journal.events[persistenceId]
	idx := k.journal.readIndex[persistenceId]
	if idx >= len(events) {
		return fmt.Errorf("no more persisted events for %q (read %d of %d)",
			persistenceId, idx, len(events))
	}

	actual := events[idx].Payload
	k.journal.readIndex[persistenceId] = idx + 1

	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("event mismatch for %q at index %d: got %v (%T), want %v (%T)",
			persistenceId, idx, actual, actual, expected, expected)
	}
	return nil
}

// PersistedInStorage returns all persisted event envelopes for the given
// persistenceId.
func (k *PersistenceTestKit) PersistedInStorage(persistenceId string) []persistence.PersistentRepr {
	k.journal.mu.Lock()
	defer k.journal.mu.Unlock()

	src := k.journal.events[persistenceId]
	out := make([]persistence.PersistentRepr, len(src))
	copy(out, src)
	return out
}

// ClearAll resets both the journal and snapshot store, removing all persisted
// data and any pending failure injections.
func (k *PersistenceTestKit) ClearAll() {
	k.journal.mu.Lock()
	k.journal.events = make(map[string][]persistence.PersistentRepr)
	k.journal.readIndex = make(map[string]int)
	k.journal.persistFailures = make(map[string]error)
	k.journal.readFailures = make(map[string]error)
	k.journal.mu.Unlock()

	k.snapshot.mu.Lock()
	k.snapshot.snapshots = make(map[string][]persistence.SelectedSnapshot)
	k.snapshot.saveFailures = make(map[string]error)
	k.snapshot.loadFailures = make(map[string]error)
	k.snapshot.mu.Unlock()
}

// ── testJournal ──────────────────────────────────────────────────────────────

type testJournal struct {
	mu              sync.Mutex
	events          map[string][]persistence.PersistentRepr
	readIndex       map[string]int // per-pid read cursor for ExpectNextPersisted
	persistFailures map[string]error
	readFailures    map[string]error
}

func newTestJournal() *testJournal {
	return &testJournal{
		events:          make(map[string][]persistence.PersistentRepr),
		readIndex:       make(map[string]int),
		persistFailures: make(map[string]error),
		readFailures:    make(map[string]error),
	}
}

func (j *testJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, msg := range messages {
		if err, ok := j.persistFailures[msg.PersistenceID]; ok {
			delete(j.persistFailures, msg.PersistenceID)
			return err
		}
		j.events[msg.PersistenceID] = append(j.events[msg.PersistenceID], msg)
	}
	return nil
}

func (j *testJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSeqNr, toSeqNr uint64, max uint64, callback func(persistence.PersistentRepr)) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err, ok := j.readFailures[persistenceId]; ok {
		delete(j.readFailures, persistenceId)
		return err
	}

	msgs := j.events[persistenceId]
	count := uint64(0)
	for _, msg := range msgs {
		if msg.SequenceNr >= fromSeqNr && msg.SequenceNr <= toSeqNr {
			callback(msg)
			count++
			if max > 0 && count >= max {
				break
			}
		}
	}
	return nil
}

func (j *testJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSeqNr uint64) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	msgs := j.events[persistenceId]
	if len(msgs) == 0 {
		return 0, nil
	}
	return msgs[len(msgs)-1].SequenceNr, nil
}

func (j *testJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSeqNr uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	msgs := j.events[persistenceId]
	var kept []persistence.PersistentRepr
	for _, msg := range msgs {
		if msg.SequenceNr > toSeqNr {
			kept = append(kept, msg)
		}
	}
	j.events[persistenceId] = kept
	return nil
}

// ── testSnapshotStore ────────────────────────────────────────────────────────

type testSnapshotStore struct {
	mu           sync.Mutex
	snapshots    map[string][]persistence.SelectedSnapshot
	saveFailures map[string]error
	loadFailures map[string]error
}

func newTestSnapshotStore() *testSnapshotStore {
	return &testSnapshotStore{
		snapshots:    make(map[string][]persistence.SelectedSnapshot),
		saveFailures: make(map[string]error),
		loadFailures: make(map[string]error),
	}
}

func (s *testSnapshotStore) SaveSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata, snapshot any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.saveFailures[metadata.PersistenceID]; ok {
		delete(s.saveFailures, metadata.PersistenceID)
		return err
	}

	s.snapshots[metadata.PersistenceID] = append(s.snapshots[metadata.PersistenceID], persistence.SelectedSnapshot{
		Metadata: metadata,
		Snapshot: snapshot,
	})
	return nil
}

func (s *testSnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria persistence.SnapshotSelectionCriteria) (*persistence.SelectedSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.loadFailures[persistenceId]; ok {
		delete(s.loadFailures, persistenceId)
		return nil, err
	}

	snaps := s.snapshots[persistenceId]
	if len(snaps) == 0 {
		return nil, nil
	}

	for i := len(snaps) - 1; i >= 0; i-- {
		snap := snaps[i]
		if snap.Metadata.SequenceNr <= criteria.MaxSequenceNr &&
			snap.Metadata.Timestamp <= criteria.MaxTimestamp &&
			snap.Metadata.SequenceNr >= criteria.MinSequenceNr &&
			snap.Metadata.Timestamp >= criteria.MinTimestamp {
			return &snap, nil
		}
	}
	return nil, nil
}

func (s *testSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps := s.snapshots[metadata.PersistenceID]
	var kept []persistence.SelectedSnapshot
	for _, snap := range snaps {
		if snap.Metadata.SequenceNr != metadata.SequenceNr {
			kept = append(kept, snap)
		}
	}
	s.snapshots[metadata.PersistenceID] = kept
	return nil
}

func (s *testSnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria persistence.SnapshotSelectionCriteria) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps := s.snapshots[persistenceId]
	var kept []persistence.SelectedSnapshot
	for _, snap := range snaps {
		if snap.Metadata.SequenceNr > criteria.MaxSequenceNr ||
			snap.Metadata.Timestamp > criteria.MaxTimestamp ||
			snap.Metadata.SequenceNr < criteria.MinSequenceNr ||
			snap.Metadata.Timestamp < criteria.MinTimestamp {
			kept = append(kept, snap)
		}
	}
	s.snapshots[persistenceId] = kept
	return nil
}

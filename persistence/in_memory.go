/*
 * in_memory.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"sync"
)

// InMemoryJournal is an in-memory implementation of the Journal interface.
type InMemoryJournal struct {
	mu       sync.RWMutex
	messages map[string][]PersistentRepr
}

func NewInMemoryJournal() *InMemoryJournal {
	return &InMemoryJournal{
		messages: make(map[string][]PersistentRepr),
	}
}

func (j *InMemoryJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	j.mu.RLock()
	defer j.mu.RUnlock()

	msgs, ok := j.messages[persistenceId]
	if !ok {
		return nil
	}

	count := uint64(0)
	for _, msg := range msgs {
		if msg.SequenceNr >= fromSequenceNr && msg.SequenceNr <= toSequenceNr {
			callback(msg)
			count++
			if max > 0 && count >= max {
				break
			}
		}
	}
	return nil
}

func (j *InMemoryJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	msgs, ok := j.messages[persistenceId]
	if !ok {
		return 0, nil
	}

	if len(msgs) == 0 {
		return 0, nil
	}

	return msgs[len(msgs)-1].SequenceNr, nil
}

func (j *InMemoryJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, msg := range messages {
		j.messages[msg.PersistenceID] = append(j.messages[msg.PersistenceID], msg)
	}
	return nil
}

func (j *InMemoryJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	msgs, ok := j.messages[persistenceId]
	if !ok {
		return nil
	}

	var newMsgs []PersistentRepr
	for _, msg := range msgs {
		if msg.SequenceNr > toSequenceNr {
			newMsgs = append(newMsgs, msg)
		}
	}
	j.messages[persistenceId] = newMsgs
	return nil
}

// InMemorySnapshotStore is an in-memory implementation of the SnapshotStore interface.
type InMemorySnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string][]SelectedSnapshot
}

func NewInMemorySnapshotStore() *InMemorySnapshotStore {
	return &InMemorySnapshotStore{
		snapshots: make(map[string][]SelectedSnapshot),
	}
}

func (s *InMemorySnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snaps, ok := s.snapshots[persistenceId]
	if !ok || len(snaps) == 0 {
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

func (s *InMemorySnapshotStore) SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshots[metadata.PersistenceID] = append(s.snapshots[metadata.PersistenceID], SelectedSnapshot{
		Metadata: metadata,
		Snapshot: snapshot,
	})
	return nil
}

func (s *InMemorySnapshotStore) DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps, ok := s.snapshots[metadata.PersistenceID]
	if !ok {
		return nil
	}

	var newSnaps []SelectedSnapshot
	for _, snap := range snaps {
		if snap.Metadata.SequenceNr != metadata.SequenceNr {
			newSnaps = append(newSnaps, snap)
		}
	}
	s.snapshots[metadata.PersistenceID] = newSnaps
	return nil
}

func (s *InMemorySnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps, ok := s.snapshots[persistenceId]
	if !ok {
		return nil
	}

	var newSnaps []SelectedSnapshot
	for _, snap := range snaps {
		if snap.Metadata.SequenceNr > criteria.MaxSequenceNr ||
			snap.Metadata.Timestamp > criteria.MaxTimestamp ||
			snap.Metadata.SequenceNr < criteria.MinSequenceNr ||
			snap.Metadata.Timestamp < criteria.MinTimestamp {
			newSnaps = append(newSnaps, snap)
		}
	}
	s.snapshots[persistenceId] = newSnaps
	return nil
}

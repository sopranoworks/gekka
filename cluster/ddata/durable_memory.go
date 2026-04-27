/*
 * durable_memory.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"sync"
)

// MemoryDurableStore is the in-memory reference implementation of
// DurableStore.  It satisfies the interface contract for tests and for
// configurations that enable durable.keys without supplying an on-disk
// backend (S22 ships the BoltDB-backed implementation).
//
// Although the store does not persist across process restarts, it does
// preserve state across Replicator.Stop / Start cycles within the same
// process — the integration tests rely on that to cover the recovery
// codepath without touching disk.
type MemoryDurableStore struct {
	mu      sync.RWMutex
	entries map[string]DurableEntry
}

// NewMemoryDurableStore returns a fresh in-memory DurableStore.
func NewMemoryDurableStore() *MemoryDurableStore {
	return &MemoryDurableStore{entries: make(map[string]DurableEntry)}
}

// Load implements DurableStore.
func (s *MemoryDurableStore) Load(_ context.Context, key string) (DurableEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok {
		return DurableEntry{}, ErrDurableKeyNotFound
	}
	return cloneEntry(e), nil
}

// LoadAll implements DurableStore.
func (s *MemoryDurableStore) LoadAll(_ context.Context) ([]DurableEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DurableEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, cloneEntry(e))
	}
	return out, nil
}

// Store implements DurableStore.
func (s *MemoryDurableStore) Store(_ context.Context, entry DurableEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.Key] = cloneEntry(entry)
	return nil
}

// Delete implements DurableStore.
func (s *MemoryDurableStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

// Close implements DurableStore.  The in-memory store has no resources to
// release, so Close is a no-op that drops the entries map (so a closed
// store reports nothing on subsequent LoadAll calls).
func (s *MemoryDurableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
	return nil
}

// Len reports how many entries are currently stored. Test-only helper.
func (s *MemoryDurableStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func cloneEntry(e DurableEntry) DurableEntry {
	cp := DurableEntry{Key: e.Key, Type: e.Type}
	if len(e.Payload) > 0 {
		cp.Payload = append([]byte(nil), e.Payload...)
	}
	return cp
}

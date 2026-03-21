/*
 * lwwmap.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"sync"
	"time"
)

// LWWEntry represents a value with a timestamp for Last-Write-Wins semantics.
type LWWEntry struct {
	Value     any   `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

// LWWMap is a convergent replicated data type where the last write wins based on timestamp.
//
// LWWMap implements DeltaReplicatedData: Put operations accumulate the changed
// entries in a delta map so the replicator can gossip only the new/updated keys.
type LWWMap struct {
	mu      sync.RWMutex
	entries map[string]LWWEntry
	delta   map[string]LWWEntry // entries written since last ResetDelta
}

// NewLWWMap creates a new LWWMap.
func NewLWWMap() *LWWMap {
	return &LWWMap{
		entries: make(map[string]LWWEntry),
		delta:   make(map[string]LWWEntry),
	}
}

// Put adds or updates a value in the map with the current timestamp.
func (m *LWWMap) Put(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := LWWEntry{
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
	m.entries[key] = entry
	m.delta[key] = entry
}

// Get retrieves a value from the map.
func (m *LWWMap) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return entry.Value, true
}

// Entries returns a copy of all entries in the map.
func (m *LWWMap) Entries() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]any, len(m.entries))
	for k, v := range m.entries {
		res[k] = v.Value
	}
	return res
}

// Snapshot returns a copy of the internal state for replication.
func (m *LWWMap) Snapshot() map[string]LWWEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]LWWEntry, len(m.entries))
	for k, v := range m.entries {
		res[k] = v
	}
	return res
}

// Merge combines another LWWMap's state into this one.
func (m *LWWMap) Merge(other map[string]LWWEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range other {
		existing, ok := m.entries[k]
		if !ok || v.Timestamp > existing.Timestamp {
			m.entries[k] = v
		}
	}
}

// MergeLWWMapDelta merges an incoming LWWMapDelta into this map.
func (m *LWWMap) MergeLWWMapDelta(d LWWMapDelta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range d.Changed {
		existing, ok := m.entries[k]
		if !ok || v.Timestamp > existing.Timestamp {
			m.entries[k] = v
		}
	}
}

// DeltaPayload implements DeltaReplicatedData.
func (m *LWWMap) DeltaPayload() (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.delta) == 0 {
		return nil, false
	}
	changed := make(map[string]LWWEntry, len(m.delta))
	for k, v := range m.delta {
		changed[k] = v
	}
	return LWWMapDelta{Changed: changed}, true
}

// ResetDelta implements DeltaReplicatedData.
func (m *LWWMap) ResetDelta() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delta = make(map[string]LWWEntry)
}

// Ensure LWWMap implements DeltaReplicatedData at compile time.
var _ DeltaReplicatedData = (*LWWMap)(nil)

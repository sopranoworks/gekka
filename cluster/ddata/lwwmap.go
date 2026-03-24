/*
 * lwwmap.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// LWWMap is a convergent replicated data type where the last write wins based on timestamp.
// It is implemented as an ORMap where values are LWWRegisters.
//
// LWWMap implements DeltaReplicatedData: Put operations accumulate the changed
// entries in a delta map so the replicator can gossip only the new/updated keys.
type LWWMap struct {
	*ORMap
	delta map[string]LWWRegisterSnapshot // entries written since last ResetDelta
}

// NewLWWMap creates a new LWWMap.
func NewLWWMap() *LWWMap {
	return &LWWMap{
		ORMap: NewORMap(),
		delta: make(map[string]LWWRegisterSnapshot),
	}
}

// Put adds or updates a value in the map with the current timestamp.
// This method is for backwards compatibility and uses a default nodeID if not provided.
// Prefer PutWithNodeID for multi-node correctness.
func (m *LWWMap) Put(key string, value any) {
	m.PutWithNodeID("local", key, value)
}

// PutWithNodeID adds or updates a value in the map with the given nodeID and current timestamp.
func (m *LWWMap) PutWithNodeID(nodeID, key string, value any) {
	m.ORMap.mu.Lock()
	defer m.ORMap.mu.Unlock()

	m.ORMap.keys.Add(nodeID, key)
	reg, ok := m.ORMap.data[key].(*LWWRegister)
	if !ok {
		reg = NewLWWRegister()
		m.ORMap.data[key] = reg
	}
	reg.Set(nodeID, value)
	m.delta[key] = reg.Snapshot()
}

// Get retrieves a value from the map.
func (m *LWWMap) Get(key string) (any, bool) {
	v, ok := m.ORMap.Get(key)
	if !ok {
		return nil, false
	}
	reg, ok := v.(*LWWRegister)
	if !ok {
		return nil, false
	}
	return reg.Get()
}

// Entries returns a copy of all entries in the map.
func (m *LWWMap) Entries() map[string]any {
	m.ORMap.mu.RLock()
	defer m.ORMap.mu.RUnlock()
	res := make(map[string]any, len(m.ORMap.data))
	for k, v := range m.ORMap.data {
		if m.ORMap.keys.Contains(k) {
			if reg, ok := v.(*LWWRegister); ok {
				if val, ok := reg.Get(); ok {
					res[k] = val
				}
			}
		}
	}
	return res
}

// Snapshot returns a copy of the internal state for replication.
// This matches the old API returning map[string]LWWEntry for compatibility with existing tests/replicator.
func (m *LWWMap) Snapshot() map[string]LWWEntry {
	m.ORMap.mu.RLock()
	defer m.ORMap.mu.RUnlock()
	res := make(map[string]LWWEntry, len(m.ORMap.data))
	for k, v := range m.ORMap.data {
		if m.ORMap.keys.Contains(k) {
			if reg, ok := v.(*LWWRegister); ok {
				snap := reg.Snapshot()
				res[k] = LWWEntry{
					Value:     snap.Value,
					Timestamp: snap.Timestamp,
				}
			}
		}
	}
	return res
}

// LWWEntry represents a value with a timestamp for Last-Write-Wins semantics.
// (Maintained for API compatibility)
type LWWEntry struct {
	Value     any   `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

// Merge combines another LWWMap's state into this one.
// (Adapted to use ORMap.Merge but accepts the old Snapshot format for compatibility)
func (m *LWWMap) Merge(other map[string]LWWEntry) {
	// Construct a temporary ORMap to leverage its Merge logic
	otherMap := NewLWWMap()
	for k, v := range other {
		// We don't have the nodeID in LWWEntry, so we use a placeholder.
		// In a real gossip, the HandleIncoming would use the proper Serializer
		// which preserves the LWWRegister full state.
		reg := NewLWWRegister()
		reg.setLocked("remote", v.Value, v.Timestamp)
		otherMap.ORMap.Put("remote", k, reg)
	}
	m.ORMap.Merge(otherMap.ORMap)
}

// MergeLWWMapDelta merges an incoming LWWMapDelta into this map.
func (m *LWWMap) MergeLWWMapDelta(d LWWMapDelta) {
	m.ORMap.mu.Lock()
	defer m.ORMap.mu.Unlock()
	for k, v := range d.Changed {
		// Note: We need to adapt LWWMapDelta to carry LWWRegisterSnapshot for full compatibility.
		// But delta.go has LWWMapDelta with map[string]LWWEntry.
		// Let's assume for now it's still LWWEntry but we treat it as LWW with "remote" nodeID.
		m.ORMap.keys.Add("remote", k)
		reg, ok := m.ORMap.data[k].(*LWWRegister)
		if !ok {
			reg = NewLWWRegister()
			m.ORMap.data[k] = reg
		}
		reg.MergeSnapshot(LWWRegisterSnapshot{
			Value:     v.Value,
			Timestamp: v.Timestamp,
			NodeID:    "remote",
		})
	}
}

// DeltaPayload implements DeltaReplicatedData.
func (m *LWWMap) DeltaPayload() (any, bool) {
	m.ORMap.mu.RLock()
	defer m.ORMap.mu.RUnlock()
	if len(m.delta) == 0 {
		return nil, false
	}
	changed := make(map[string]LWWEntry, len(m.delta))
	for k, v := range m.delta {
		changed[k] = LWWEntry{
			Value:     v.Value,
			Timestamp: v.Timestamp,
		}
	}
	return LWWMapDelta{Changed: changed}, true
}

// ResetDelta implements DeltaReplicatedData.
func (m *LWWMap) ResetDelta() {
	m.ORMap.mu.Lock()
	defer m.ORMap.mu.Unlock()
	m.delta = make(map[string]LWWRegisterSnapshot)
}

// Ensure LWWMap implements DeltaReplicatedData at compile time.
var _ DeltaReplicatedData = (*LWWMap)(nil)

/*
 * ormap.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"sync"
)

// ReplicatedData represents a CRDT that can be merged with another instance.
type ReplicatedData interface {
	Merge(other ReplicatedData) ReplicatedData
	Copy() ReplicatedData
}

// ORMap is an Observed-Remove Map CRDT.
// It maps keys to other ReplicatedData instances.
// It uses an underlying ORSet to track the set of keys.
type ORMap struct {
	mu   sync.RWMutex
	keys *ORSet
	data map[string]ReplicatedData
}

// NewORMap creates an empty ORMap.
func NewORMap() *ORMap {
	return &ORMap{
		keys: NewORSet(),
		data: make(map[string]ReplicatedData),
	}
}

// Put adds or updates a value in the map.
func (m *ORMap) Put(nodeID, key string, value ReplicatedData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys.Add(nodeID, key)
	m.data[key] = value
}

// Remove removes a key from the map.
func (m *ORMap) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys.Remove(key)
	delete(m.data, key)
}

// Get retrieves a value from the map.
func (m *ORMap) Get(key string) (ReplicatedData, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.keys.Contains(key) {
		return nil, false
	}
	v, ok := m.data[key]
	return v, ok
}

// Entries returns a copy of all entries currently in the map.
func (m *ORMap) Entries() map[string]ReplicatedData {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]ReplicatedData, len(m.data))
	for k, v := range m.data {
		if m.keys.Contains(k) {
			res[k] = v
		}
	}
	return res
}

// Merge combines another ORMap into this one.
func (m *ORMap) Merge(other ReplicatedData) ReplicatedData {
	o, ok := other.(*ORMap)
	if !ok {
		return m
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	o.mu.RLock()
	defer o.mu.RUnlock()

	// Merge keys set using the underlying dots/vv for full convergence
	m.keys.MergeSnapshot(o.keys.Snapshot())

	// Merge data
	for k, v := range o.data {
		if existing, ok := m.data[k]; ok {
			m.data[k] = existing.Merge(v)
		} else {
			m.data[k] = v.Copy()
		}
	}

	// Filter data by merged keys
	currentKeys := m.keys.Elements()
	keySet := make(map[string]struct{}, len(currentKeys))
	for _, k := range currentKeys {
		keySet[k] = struct{}{}
	}
	for k := range m.data {
		if _, ok := keySet[k]; !ok {
			delete(m.data, k)
		}
	}

	return m
}

func (m *ORMap) Copy() ReplicatedData {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := NewORMap()
	res.keys = m.keys.Copy().(*ORSet)
	for k, v := range m.data {
		res.data[k] = v.Copy()
	}
	return res
}

// PNCounterMap is a specialized ORMap where values are PNCounters.
type PNCounterMap struct {
	ORMap
}

func NewPNCounterMap() *PNCounterMap {
	return &PNCounterMap{ORMap: *NewORMap()}
}

func (m *PNCounterMap) Increment(nodeID, key string, delta uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys.Add(nodeID, key)
	v, ok := m.data[key]
	var counter *PNCounter
	if !ok {
		counter = NewPNCounter()
		m.data[key] = counter
	} else {
		counter = v.(*PNCounter)
	}
	counter.Increment(nodeID, delta)
}

func (m *PNCounterMap) Decrement(nodeID, key string, delta uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys.Add(nodeID, key)
	v, ok := m.data[key]
	var counter *PNCounter
	if !ok {
		counter = NewPNCounter()
		m.data[key] = counter
	} else {
		counter = v.(*PNCounter)
	}
	counter.Decrement(nodeID, delta)
}

func (m *PNCounterMap) Merge(other ReplicatedData) ReplicatedData {
	o := other.(*PNCounterMap)
	m.ORMap.Merge(&o.ORMap)
	return m
}

func (m *PNCounterMap) Copy() ReplicatedData {
	copied := m.ORMap.Copy().(*ORMap)
	result := &PNCounterMap{}
	result.keys = copied.keys
	result.data = copied.data
	return result
}

// ORMultiMap is a specialized ORMap where values are ORSets.
type ORMultiMap struct {
	ORMap
}

func NewORMultiMap() *ORMultiMap {
	return &ORMultiMap{ORMap: *NewORMap()}
}

func (m *ORMultiMap) AddBinding(nodeID, key, element string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys.Add(nodeID, key)
	v, ok := m.data[key]
	var set *ORSet
	if !ok {
		set = NewORSet()
		m.data[key] = set
	} else {
		set = v.(*ORSet)
	}
	set.Add(nodeID, element)
}

func (m *ORMultiMap) RemoveBinding(nodeID, key, element string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if ok {
		set := v.(*ORSet)
		set.Remove(element)
		if len(set.Elements()) == 0 {
			m.keys.Remove(key)
			delete(m.data, key)
		}
	}
}

func (m *ORMultiMap) Merge(other ReplicatedData) ReplicatedData {
	o := other.(*ORMultiMap)
	m.ORMap.Merge(&o.ORMap)
	return m
}

func (m *ORMultiMap) Copy() ReplicatedData {
	copied := m.ORMap.Copy().(*ORMap)
	result := &ORMultiMap{}
	result.keys = copied.keys
	result.data = copied.data
	return result
}

// Ensure implementations satisfy ReplicatedData
var _ ReplicatedData = (*ORMap)(nil)
var _ ReplicatedData = (*PNCounterMap)(nil)
var _ ReplicatedData = (*ORMultiMap)(nil)
var _ ReplicatedData = (*PNCounter)(nil)
var _ ReplicatedData = (*ORSet)(nil)
var _ ReplicatedData = (*LWWRegister)(nil)

func (p *PNCounter) Merge(other ReplicatedData) ReplicatedData {
	o := other.(*PNCounter)
	p.MergeSnapshot(o.Snapshot())
	return p
}

func (p *PNCounter) Copy() ReplicatedData {
	res := NewPNCounter()
	res.MergeSnapshot(p.Snapshot())
	return res
}

func (s *ORSet) Merge(other ReplicatedData) ReplicatedData {
	o := other.(*ORSet)
	s.MergeSnapshot(o.Snapshot())
	return s
}

func (s *ORSet) Copy() ReplicatedData {
	res := NewORSet()
	res.MergeSnapshot(s.Snapshot())
	return res
}

func (r *LWWRegister) Merge(other ReplicatedData) ReplicatedData {
	o := other.(*LWWRegister)
	r.MergeSnapshot(o.Snapshot())
	return r
}

func (r *LWWRegister) Copy() ReplicatedData {
	res := NewLWWRegister()
	res.MergeSnapshot(r.Snapshot())
	return res
}

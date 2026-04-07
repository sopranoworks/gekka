/*
 * gset.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"sort"
	"sync"
)

// GSet is a Grow-only Set CRDT. Elements can only be added, never removed.
// Merge is set union, which is commutative, associative, and idempotent.
//
// GSet implements DeltaReplicatedData: each Add call records the newly added
// elements in a delta accumulator. The replicator retrieves the delta via
// DeltaPayload and clears it with ResetDelta after gossip.
type GSet struct {
	mu       sync.RWMutex
	elements map[string]struct{}
	delta    map[string]struct{} // elements added since last ResetDelta
}

// NewGSet creates a new empty GSet.
func NewGSet() *GSet {
	return &GSet{
		elements: make(map[string]struct{}),
		delta:    make(map[string]struct{}),
	}
}

// Add inserts an element into the set. If the element already exists, this is
// a no-op for the state but still records it in the delta (idempotent merge
// on the receiver handles deduplication).
func (g *GSet) Add(elem string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.elements[elem]; !exists {
		g.elements[elem] = struct{}{}
		g.delta[elem] = struct{}{}
	}
}

// Contains reports whether the set contains elem.
func (g *GSet) Contains(elem string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.elements[elem]
	return ok
}

// Elements returns all elements in the set in sorted order.
func (g *GSet) Elements() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]string, 0, len(g.elements))
	for e := range g.elements {
		out = append(out, e)
	}
	sort.Strings(out)
	return out
}

// Size returns the number of elements in the set.
func (g *GSet) Size() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.elements)
}

// Merge returns a new GSet that is the union of g and other.
func (g *GSet) Merge(other *GSet) *GSet {
	g.mu.RLock()
	defer g.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	result := NewGSet()
	for e := range g.elements {
		result.elements[e] = struct{}{}
	}
	for e := range other.elements {
		result.elements[e] = struct{}{}
	}
	return result
}

// MergeDelta applies a GSetDelta to this GSet, adding all elements from the
// delta. This is used by the replicator on the receiving side.
func (g *GSet) MergeDelta(d *GSetDelta) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, e := range d.Added {
		g.elements[e] = struct{}{}
	}
}

// ── DeltaReplicatedData ──────────────────────────────────────────────────────

// DeltaPayload returns the elements added since the last ResetDelta as a
// GSetDelta. Returns (nil, false) when no elements have been added.
func (g *GSet) DeltaPayload() (any, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.delta) == 0 {
		return nil, false
	}
	added := make([]string, 0, len(g.delta))
	for e := range g.delta {
		added = append(added, e)
	}
	sort.Strings(added)
	return &GSetDelta{Added: added}, true
}

// ResetDelta clears the accumulated delta.
func (g *GSet) ResetDelta() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.delta = make(map[string]struct{})
}

// GSetDelta carries the elements added since the last ResetDelta.
type GSetDelta struct {
	Added []string `json:"added"`
}

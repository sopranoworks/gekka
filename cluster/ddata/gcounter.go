/*
 * gcounter.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import "sync"

// GCounter is a Grow-only Counter CRDT.
// Each node can only increment its own slot; the total value is the sum of all slots.
// Merge is pairwise max, which satisfies commutativity, associativity, and idempotency.
//
// GCounter implements DeltaReplicatedData: each Increment call accumulates the
// incremental value in a separate delta map.  The replicator calls DeltaPayload
// to retrieve the delta and ResetDelta to clear it after gossip.
type GCounter struct {
	mu    sync.RWMutex
	state map[string]uint64 // nodeID -> value
	delta map[string]uint64 // accumulated increments since last ResetDelta
}

func NewGCounter() *GCounter {
	return &GCounter{
		state: make(map[string]uint64),
		delta: make(map[string]uint64),
	}
}

// Increment adds d to this node's slot and records it in the delta accumulator.
func (g *GCounter) Increment(nodeID string, d uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state[nodeID] += d
	g.delta[nodeID] += d
}

// Value returns the total count across all nodes.
func (g *GCounter) Value() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var total uint64
	for _, v := range g.state {
		total += v
	}
	return total
}

// NodeValue returns the count for a specific node.
func (g *GCounter) NodeValue(nodeID string) uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state[nodeID]
}

// Snapshot returns a copy of the internal state (nodeID -> value).
func (g *GCounter) Snapshot() map[string]uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string]uint64, len(g.state))
	for k, v := range g.state {
		out[k] = v
	}
	return out
}

// Merge produces a new GCounter whose state is the pairwise max of g and other.
// The result satisfies eventual consistency: Merge(A, B) == Merge(B, A).
func (g *GCounter) Merge(other *GCounter) *GCounter {
	g.mu.RLock()
	defer g.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	result := NewGCounter()
	for k, v := range g.state {
		result.state[k] = v
	}
	for k, v := range other.state {
		if v > result.state[k] {
			result.state[k] = v
		}
	}
	return result
}

// MergeState merges a raw snapshot (from a gossip message) into this counter in place.
func (g *GCounter) MergeState(incoming map[string]uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, v := range incoming {
		if v > g.state[k] {
			g.state[k] = v
		}
	}
}

// MergeCounterDelta merges an incoming GCounterDelta into this counter.
// Delta values are added to the current slot values (clamped so the total
// never decreases — identical semantics to pairwise-max on absolute values).
func (g *GCounter) MergeCounterDelta(d GCounterDelta) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, inc := range d.Delta {
		if g.state[k]+inc > g.state[k] { // overflow guard
			g.state[k] += inc
		}
	}
}

// DeltaPayload implements DeltaReplicatedData. Returns the accumulated delta
// since the last ResetDelta call. Returns (nil, false) when no increments have
// occurred since the last reset.
func (g *GCounter) DeltaPayload() (any, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.delta) == 0 {
		return nil, false
	}
	d := make(map[string]uint64, len(g.delta))
	for k, v := range g.delta {
		d[k] = v
	}
	return GCounterDelta{Delta: d}, true
}

// ResetDelta implements DeltaReplicatedData. Clears the accumulated delta.
func (g *GCounter) ResetDelta() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.delta = make(map[string]uint64)
}

// NeedsPruning implements Prunable. Returns true when the counter still
// holds a slot for removedNode.
func (g *GCounter) NeedsPruning(removedNode string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.state[removedNode]
	return exists
}

// Prune transfers the removedNode's slot onto collapseInto. The delta
// accumulator is also rewritten so any in-flight delta message won't
// re-announce the removed node.
func (g *GCounter) Prune(removedNode, collapseInto string) {
	if removedNode == "" || collapseInto == "" || removedNode == collapseInto {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if val, exists := g.state[removedNode]; exists {
		g.state[collapseInto] += val
		delete(g.state, removedNode)
	}
	if val, exists := g.delta[removedNode]; exists {
		g.delta[collapseInto] += val
		delete(g.delta, removedNode)
	}
}

// Ensure GCounter implements DeltaReplicatedData and Prunable at compile time.
var (
	_ DeltaReplicatedData = (*GCounter)(nil)
	_ Prunable            = (*GCounter)(nil)
)

/*
 * gcounter.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package crdt

import "sync"

// GCounter is a Grow-only Counter CRDT.
// Each node can only increment its own slot; the total value is the sum of all slots.
// Merge is pairwise max, which satisfies commutativity, associativity, and idempotency.
type GCounter struct {
	mu    sync.RWMutex
	state map[string]uint64 // nodeID -> value
}

func NewGCounter() *GCounter {
	return &GCounter{state: make(map[string]uint64)}
}

// Increment adds delta to this node's slot.
func (g *GCounter) Increment(nodeID string, delta uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state[nodeID] += delta
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

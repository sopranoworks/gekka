/*
 * pncounter.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import "sync"

// PNCounter is a Positive-Negative Counter CRDT.
//
// It combines two GCounters internally:
//   - pos tracks all increments (each node's cumulative add total)
//   - neg tracks all decrements (each node's cumulative subtract total)
//
// The observable value is pos.Value() - neg.Value().
//
// Like GCounter, PNCounter satisfies commutativity, associativity, and
// idempotency — making it safe to gossip to peers in any order.
type PNCounter struct {
	mu  sync.RWMutex
	pos *GCounter // increments
	neg *GCounter // decrements
}

// NewPNCounter creates a zero-valued PNCounter.
func NewPNCounter() *PNCounter {
	return &PNCounter{
		pos: NewGCounter(),
		neg: NewGCounter(),
	}
}

// Increment adds delta to nodeID's positive slot.
func (p *PNCounter) Increment(nodeID string, delta uint64) {
	p.pos.Increment(nodeID, delta)
}

// Decrement adds delta to nodeID's negative slot.
func (p *PNCounter) Decrement(nodeID string, delta uint64) {
	p.neg.Increment(nodeID, delta)
}

// Value returns the net count: sum(pos) - sum(neg).
// The result is int64; it can be negative when total decrements exceed total increments.
func (p *PNCounter) Value() int64 {
	return int64(p.pos.Value()) - int64(p.neg.Value())
}

// PNCounterSnapshot is the serialisable gossip payload for a PNCounter.
type PNCounterSnapshot struct {
	Pos map[string]uint64 `json:"pos"`
	Neg map[string]uint64 `json:"neg"`
}

// Snapshot returns a serialisable copy of the internal state.
func (p *PNCounter) Snapshot() PNCounterSnapshot {
	return PNCounterSnapshot{
		Pos: p.pos.Snapshot(),
		Neg: p.neg.Snapshot(),
	}
}

// MergeSnapshot merges an incoming gossip snapshot into this counter in place.
// The operation is a pairwise max on both the pos and neg GCounters.
func (p *PNCounter) MergeSnapshot(snap PNCounterSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pos.MergeState(snap.Pos)
	p.neg.MergeState(snap.Neg)
}

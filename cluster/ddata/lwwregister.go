/*
 * lwwregister.go
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

// LWWRegister is a Last-Write-Wins Register CRDT.
//
// Each write is stamped with a wall-clock nanosecond timestamp and the
// writing node's ID.  Merge picks the entry with the highest timestamp;
// when two entries share the same timestamp the node ID with higher
// lexical order wins, providing a deterministic tie-break without
// requiring clock synchronisation.
//
// Because the value is "any", it is serialised as JSON for gossip.
// Numeric types received over the wire will be float64 after unmarshal,
// matching Go's standard json.Unmarshal behaviour.
type LWWRegister struct {
	mu        sync.RWMutex
	value     any
	timestamp int64  // UnixNano of the winning write
	nodeID    string // writer that produced the winning value
}

// NewLWWRegister returns an empty register (value nil, timestamp 0).
func NewLWWRegister() *LWWRegister {
	return &LWWRegister{}
}

// Set stores value as the new register content, stamped with the current
// wall-clock time and the caller's nodeID.
func (r *LWWRegister) Set(nodeID string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := time.Now().UnixNano()
	r.setLocked(nodeID, value, ts)
}

// setLocked updates the register if the new (ts, nodeID) pair beats the
// current entry.  Must be called with r.mu held.
func (r *LWWRegister) setLocked(nodeID string, value any, ts int64) {
	if ts > r.timestamp ||
		(ts == r.timestamp && nodeID > r.nodeID) {
		r.value = value
		r.timestamp = ts
		r.nodeID = nodeID
	}
}

// Get returns the current value and whether the register has ever been set.
func (r *LWWRegister) Get() (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value, r.timestamp != 0
}

// LWWRegisterSnapshot is the serialisable gossip payload for an LWWRegister.
type LWWRegisterSnapshot struct {
	Value     any    `json:"value"`
	Timestamp int64  `json:"timestamp"`
	NodeID    string `json:"node_id"`
}

// Snapshot returns a serialisable copy of the current register state.
func (r *LWWRegister) Snapshot() LWWRegisterSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return LWWRegisterSnapshot{
		Value:     r.value,
		Timestamp: r.timestamp,
		NodeID:    r.nodeID,
	}
}

// MergeSnapshot merges an incoming gossip snapshot using LWW + nodeID
// tie-break semantics.
func (r *LWWRegister) MergeSnapshot(snap LWWRegisterSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setLocked(snap.NodeID, snap.Value, snap.Timestamp)
}

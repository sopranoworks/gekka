/*
 * delta.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// DeltaReplicatedData is implemented by CRDTs that support delta propagation.
//
// Instead of sending the full state on every gossip cycle, a delta-aware CRDT
// accumulates only the changes (inserts, increments, updates) since the last
// reset.  The replicator calls DeltaPayload to retrieve the serialisable delta,
// sends it to peers using a compact message type (e.g. "gcounter-delta"), and
// then calls ResetDelta to clear the accumulator.
//
// On the receiving side, HandleIncoming dispatches the delta payload to the
// appropriate concrete merge method (MergeCounterDelta, MergeORSetDelta,
// MergeLWWMapDelta).
//
// Correctness guarantee: the replicator falls back to a full-state gossip every
// fullStateEvery rounds so that any deltas dropped in transit are eventually
// repaired.
type DeltaReplicatedData interface {
	// DeltaPayload returns a JSON-serialisable value representing the
	// accumulated changes since the last ResetDelta call, and a boolean
	// indicating whether there are any pending changes.  Returns (nil, false)
	// when the CRDT has not changed since the last reset.
	DeltaPayload() (any, bool)

	// ResetDelta clears the accumulated delta.  Called by the replicator after
	// a successful delta gossip round.
	ResetDelta()
}

// ── Delta payload types ───────────────────────────────────────────────────────

// GCounterDelta carries only the node slots that changed since the last reset.
type GCounterDelta struct {
	Delta map[string]uint64 `json:"delta"` // nodeID -> incremental value
}

// ORSetDelta carries the dots added and elements removed since the last reset,
// together with the updated version vector so the receiver can detect gaps.
type ORSetDelta struct {
	AddedDots       map[string][]Dot  `json:"addedDots"`       // element -> new dots
	RemovedElements []string          `json:"removedElements"` // elements explicitly removed
	VV              map[string]uint64 `json:"vv"`              // current version vector
}

// LWWMapDelta carries only the entries that were written since the last reset.
type LWWMapDelta struct {
	Changed map[string]LWWEntry `json:"changed"` // key -> updated entry
}

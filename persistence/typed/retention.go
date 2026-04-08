/*
 * retention.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

// RetentionCriteria governs automated snapshot retention and optional event
// deletion for [EventSourcedBehavior].
//
// When configured via [EventSourcedBehavior.RetentionCriteria], the persistent
// actor automatically cleans up old snapshots and events after each snapshot
// save.
type RetentionCriteria struct {
	// SnapshotEveryNEvents triggers a snapshot every N events.
	// If zero, no automatic snapshots are taken by this criteria.
	SnapshotEveryNEvents uint64

	// KeepNSnapshots is the number of most recent snapshots to retain.
	// Older snapshots are deleted after each new snapshot is saved.
	// If zero, all snapshots are retained.
	KeepNSnapshots int

	// DeleteEventsOnSnapshot, when true, deletes events up to the oldest
	// retained snapshot's sequence number after each snapshot save.
	// This reduces journal storage but makes recovery depend on at least one
	// snapshot being available.
	DeleteEventsOnSnapshot bool
}

// SnapshotEvery returns a RetentionCriteria that takes a snapshot every n
// events and retains the most recent keep snapshots.
func SnapshotEvery(n uint64, keep int) *RetentionCriteria {
	return &RetentionCriteria{
		SnapshotEveryNEvents: n,
		KeepNSnapshots:       keep,
	}
}

// WithDeleteEventsOnSnapshot returns a copy of the criteria with event deletion
// enabled.
func (rc *RetentionCriteria) WithDeleteEventsOnSnapshot() *RetentionCriteria {
	cp := *rc
	cp.DeleteEventsOnSnapshot = true
	return &cp
}

/*
 * recovery.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import "github.com/sopranoworks/gekka/persistence"

// RecoveryStrategy controls how a persistent actor recovers its state.
type RecoveryStrategy struct {
	// Disabled, when true, skips recovery entirely.  The actor starts with
	// InitialState and seqNr=0, appending new events after the journal's
	// highest sequence number.
	Disabled bool

	// SnapshotSelectionCriteria overrides the default criteria for loading the
	// recovery snapshot.  Ignored when Disabled is true.
	SnapshotSelectionCriteria *persistence.SnapshotSelectionCriteria

	// ReplayFilter, if non-nil, is called for each event during recovery.
	// Return true to apply the event, false to skip it.  Skipped events still
	// advance the sequence number but are not applied to state.
	ReplayFilter func(repr persistence.PersistentRepr) bool
}

// DisabledRecovery returns a RecoveryStrategy that skips recovery entirely.
func DisabledRecovery() *RecoveryStrategy {
	return &RecoveryStrategy{Disabled: true}
}

// RecoveryWithFilter returns a RecoveryStrategy that filters events during replay.
func RecoveryWithFilter(filter func(repr persistence.PersistentRepr) bool) *RecoveryStrategy {
	return &RecoveryStrategy{ReplayFilter: filter}
}

/*
 * state_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import "context"

// DurableStateStore defines the interface for state-based persistence.
// Unlike a Journal which stores events, a StateStore stores the latest state.
type DurableStateStore interface {
	// Get retrieves the current state and its revision for a given persistence ID.
	// If no state exists, it returns (nil, 0, nil).
	Get(ctx context.Context, persistenceID string) (state any, revision uint64, err error)

	// Upsert updates or inserts the state for a given persistence ID.
	// seqNr is used for optimistic locking/concurrency control.
	Upsert(ctx context.Context, persistenceID string, seqNr uint64, state any, tag string) error

	// Delete removes the state for a given persistence ID.
	Delete(ctx context.Context, persistenceID string) error
}

/*
 * snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"math"
)

// SnapshotMetadata contains info about a saved snapshot.
type SnapshotMetadata struct {
	PersistenceID string
	SequenceNr    uint64
	Timestamp     int64 // Unix timestamp
}

// SelectedSnapshot contains the loaded snapshot and its metadata.
type SelectedSnapshot struct {
	Metadata SnapshotMetadata
	Snapshot any
}

// SnapshotStore is the interface for storing and loading state snapshots.
type SnapshotStore interface {
	// LoadSnapshot loads the latest snapshot for the given persistenceId.
	// If a snapshot is found, it returns SelectedSnapshot.
	LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error)

	// SaveSnapshot saves a snapshot for the given persistenceId.
	SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error

	// DeleteSnapshot deletes the specified snapshot.
	DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error

	// DeleteSnapshots deletes all snapshots matching the given criteria.
	DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error
}

// SnapshotSelectionCriteria defines how to select snapshots for loading or deletion.
type SnapshotSelectionCriteria struct {
	MaxSequenceNr uint64
	MaxTimestamp  int64
	MinSequenceNr uint64
	MinTimestamp  int64
}

// LatestSnapshotCriteria returns criteria for loading the absolute latest snapshot.
func LatestSnapshotCriteria() SnapshotSelectionCriteria {
	return SnapshotSelectionCriteria{
		MaxSequenceNr: ^uint64(0),
		MaxTimestamp:  math.MaxInt64,
	}
}

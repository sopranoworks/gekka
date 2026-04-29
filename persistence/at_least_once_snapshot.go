/*
 * at_least_once_snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"errors"
	"time"
)

// ErrNoDeliverySnapshot is returned by LoadAtLeastOnceSnapshot when the
// SnapshotStore has no snapshot for the given persistence id (a fresh
// recovery — the caller should treat this as "start from empty state").
var ErrNoDeliverySnapshot = errors.New("persistence: no at-least-once delivery snapshot")

// SaveAtLeastOnceSnapshot persists the AtLeastOnceDelivery's pending set
// through the supplied SnapshotStore.  The DeliverySnapshot value is
// stored verbatim so the same SnapshotStore that handles regular
// persistent-actor snapshots can also restore at-least-once state on
// recovery.  Callers typically pass the same SnapshotStore registered in
// pekko.persistence.snapshot-store.plugin.
func SaveAtLeastOnceSnapshot(
	ctx context.Context,
	store SnapshotStore,
	persistenceID string,
	sequenceNr uint64,
	d *AtLeastOnceDelivery,
) error {
	if store == nil {
		return errors.New("persistence: SaveAtLeastOnceSnapshot: nil SnapshotStore")
	}
	if d == nil {
		return errors.New("persistence: SaveAtLeastOnceSnapshot: nil AtLeastOnceDelivery")
	}
	snap := d.GetDeliverySnapshot()
	meta := SnapshotMetadata{
		PersistenceID: persistenceID,
		SequenceNr:    sequenceNr,
		Timestamp:     time.Now().UnixMilli(),
	}
	return store.SaveSnapshot(ctx, meta, snap)
}

// LoadAtLeastOnceSnapshot loads the most recent DeliverySnapshot stored
// for persistenceID and applies it to the supplied AtLeastOnceDelivery.
// Returns ErrNoDeliverySnapshot when the store has no snapshot for the
// id; in that case the AtLeastOnceDelivery is left untouched and the
// caller can proceed with an empty pending set.
func LoadAtLeastOnceSnapshot(
	ctx context.Context,
	store SnapshotStore,
	persistenceID string,
	d *AtLeastOnceDelivery,
) (SnapshotMetadata, error) {
	if store == nil {
		return SnapshotMetadata{}, errors.New("persistence: LoadAtLeastOnceSnapshot: nil SnapshotStore")
	}
	if d == nil {
		return SnapshotMetadata{}, errors.New("persistence: LoadAtLeastOnceSnapshot: nil AtLeastOnceDelivery")
	}
	sel, err := store.LoadSnapshot(ctx, persistenceID, LatestSnapshotCriteria())
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if sel == nil {
		return SnapshotMetadata{}, ErrNoDeliverySnapshot
	}
	snap, ok := sel.Snapshot.(DeliverySnapshot)
	if !ok {
		// The snapshot store returned a value of an unexpected type —
		// either the persistence id collides with another actor's
		// snapshot or a serialization round-trip lost the type. Surface
		// it so callers can decide whether to wipe the slot.
		if ptr, isPtr := sel.Snapshot.(*DeliverySnapshot); isPtr && ptr != nil {
			d.SetDeliverySnapshot(*ptr)
			return sel.Metadata, nil
		}
		return sel.Metadata, errors.New("persistence: LoadAtLeastOnceSnapshot: snapshot is not a DeliverySnapshot")
	}
	d.SetDeliverySnapshot(snap)
	return sel.Metadata, nil
}

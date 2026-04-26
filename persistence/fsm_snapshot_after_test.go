/*
 * fsm_snapshot_after_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersistentFSM_SnapshotAfter_TriggersSnapshotAtN verifies that
// pekko.persistence.fsm.snapshot-after causes PersistentFSM to save a snapshot
// once the configured number of events has been persisted.
func TestPersistentFSM_SnapshotAfter_TriggersSnapshotAtN(t *testing.T) {
	journal := NewInMemoryJournal()
	store := NewInMemorySnapshotStore()

	a := newOrderFSMActor(journal, "order-snap-after")
	a.WithSnapshotStore(store).SetSnapshotAfter(2)
	a.PersistentFSMPreStart()

	// First persisted event — under threshold, no snapshot expected yet.
	a.Receive(PayOrder{Amount: 10.0})
	require.Equal(t, OrderPaid, a.State())

	snap, err := store.LoadSnapshot(context.Background(), "order-snap-after", LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap, "no snapshot should exist after only 1 event with snapshot-after=2")

	// Second persisted event — crosses the threshold.
	a.Receive(ShipOrder{TrackingID: "TRK-9"})
	require.Equal(t, OrderShipped, a.State())

	snap, err = store.LoadSnapshot(context.Background(), "order-snap-after", LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap, "snapshot should be saved after 2 events with snapshot-after=2")
	assert.Equal(t, uint64(2), snap.Metadata.SequenceNr, "snapshot seqNr should match last persisted event")

	payload, ok := snap.Snapshot.(FSMSnapshot[OrderState, OrderData])
	require.True(t, ok, "snapshot payload should be FSMSnapshot[OrderState, OrderData], got %T", snap.Snapshot)
	assert.Equal(t, OrderShipped, payload.State)
	assert.Equal(t, 10.0, payload.Data.Total)
}

// TestPersistentFSM_SnapshotAfter_DisabledByDefault verifies that without an
// explicit snapshot-after value, no snapshot is written even when a snapshot
// store is configured.
func TestPersistentFSM_SnapshotAfter_DisabledByDefault(t *testing.T) {
	// Reset the package-default to the documented Pekko default ("off") in
	// case a previous test or live config changed it.
	SetDefaultFSMSnapshotAfter(0)

	journal := NewInMemoryJournal()
	store := NewInMemorySnapshotStore()

	a := newOrderFSMActor(journal, "order-no-snap")
	a.WithSnapshotStore(store) // no SetSnapshotAfter call → defaults to 0
	a.PersistentFSMPreStart()

	a.Receive(PayOrder{Amount: 5.0})
	a.Receive(ShipOrder{TrackingID: "TRK-X"})

	snap, err := store.LoadSnapshot(context.Background(), "order-no-snap", LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap, "no snapshot expected when snapshot-after is 0/off")
}

// TestPersistentFSM_SnapshotAfter_RecoversFromSnapshot verifies that the saved
// snapshot is consulted on the next start so that journal replay only needs to
// process events written after the snapshot's sequence number.
func TestPersistentFSM_SnapshotAfter_RecoversFromSnapshot(t *testing.T) {
	journal := NewInMemoryJournal()
	store := NewInMemorySnapshotStore()

	// First instance: drive through Created → Paid → Shipped with snapshot-after=2.
	a1 := newOrderFSMActor(journal, "order-recover-snap")
	a1.WithSnapshotStore(store).SetSnapshotAfter(2)
	a1.PersistentFSMPreStart()
	a1.Receive(PayOrder{Amount: 33.0})
	a1.Receive(ShipOrder{TrackingID: "TRK-RECO"})
	require.Equal(t, OrderShipped, a1.State())

	// Second instance: recover. The snapshot should restore state directly
	// without needing to replay the events.
	a2 := newOrderFSMActor(journal, "order-recover-snap")
	a2.WithSnapshotStore(store).SetSnapshotAfter(2)
	a2.PersistentFSMPreStart()
	assert.Equal(t, OrderShipped, a2.State(), "recovered FSM should resume in Shipped state")
	assert.Equal(t, 33.0, a2.Data().Total)
}

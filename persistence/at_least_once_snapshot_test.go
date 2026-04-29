/*
 * at_least_once_snapshot_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAtLeastOnceSnapshot_RoundTrip(t *testing.T) {
	store := NewInMemorySnapshotStore()
	d := NewAtLeastOnceDelivery(time.Second, 100)

	// Two pending deliveries, one confirmed; expect 2 in the snapshot.
	id1 := deliverOK(t, d, "/user/a", "m1")
	deliverOK(t, d, "/user/b", "m2")
	deliverOK(t, d, "/user/c", "m3")
	require.True(t, d.ConfirmDelivery(id1))
	require.Equal(t, 2, d.UnconfirmedCount())

	require.NoError(t, SaveAtLeastOnceSnapshot(context.Background(), store, "actor-1", 1, d))

	// Restore into a fresh delivery instance and verify state matches.
	d2 := NewAtLeastOnceDelivery(time.Second, 100)
	meta, err := LoadAtLeastOnceSnapshot(context.Background(), store, "actor-1", d2)
	require.NoError(t, err)
	assert.Equal(t, "actor-1", meta.PersistenceID)
	assert.Equal(t, uint64(1), meta.SequenceNr)
	assert.Equal(t, 2, d2.UnconfirmedCount())

	// Continuing deliveries on the restored instance use the next id.
	id, err := d2.Deliver("/user/d", "m4")
	require.NoError(t, err)
	assert.Equal(t, int64(4), id)
}

func TestAtLeastOnceSnapshot_NotFound(t *testing.T) {
	store := NewInMemorySnapshotStore()
	d := NewAtLeastOnceDelivery(time.Second, 100)

	_, err := LoadAtLeastOnceSnapshot(context.Background(), store, "no-such-actor", d)
	assert.ErrorIs(t, err, ErrNoDeliverySnapshot)
}

func TestAtLeastOnceSnapshot_RecoveryAttempts(t *testing.T) {
	store := NewInMemorySnapshotStore()
	d := NewAtLeastOnceDelivery(20*time.Millisecond, 10)
	deliverOK(t, d, "/user/x", "msg")

	// Spin redelivery so the Attempts counter rises before the snapshot.
	d.StartRedelivery(func(dest string, id int64, msg any) {})
	require.Eventually(t, func() bool {
		return d.MaxAttempts() >= 2
	}, time.Second, 10*time.Millisecond)
	d.StopRedelivery()

	require.NoError(t, SaveAtLeastOnceSnapshot(context.Background(), store, "actor-attempts", 7, d))

	d2 := NewAtLeastOnceDelivery(time.Second, 10)
	_, err := LoadAtLeastOnceSnapshot(context.Background(), store, "actor-attempts", d2)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, d2.MaxAttempts(), 2)
}

func TestAtLeastOnceSnapshot_NilArgs(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 10)
	store := NewInMemorySnapshotStore()

	require.Error(t, SaveAtLeastOnceSnapshot(context.Background(), nil, "p", 1, d))
	require.Error(t, SaveAtLeastOnceSnapshot(context.Background(), store, "p", 1, nil))

	_, err := LoadAtLeastOnceSnapshot(context.Background(), nil, "p", d)
	require.Error(t, err)
	_, err = LoadAtLeastOnceSnapshot(context.Background(), store, "p", nil)
	require.Error(t, err)
}

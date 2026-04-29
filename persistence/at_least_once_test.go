/*
 * at_least_once_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deliverOK(t *testing.T, d *AtLeastOnceDelivery, dest string, msg any) int64 {
	t.Helper()
	id, err := d.Deliver(dest, msg)
	require.NoError(t, err)
	return id
}

func TestAtLeastOnceDelivery_DeliverAndConfirm(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 100)

	id1 := deliverOK(t, d, "/user/target", "msg1")
	id2 := deliverOK(t, d, "/user/target", "msg2")

	assert.Equal(t, int64(1), id1)
	assert.Equal(t, int64(2), id2)
	assert.Equal(t, 2, d.UnconfirmedCount())

	ok := d.ConfirmDelivery(id1)
	assert.True(t, ok)
	assert.Equal(t, 1, d.UnconfirmedCount())

	ok = d.ConfirmDelivery(id2)
	assert.True(t, ok)
	assert.Equal(t, 0, d.UnconfirmedCount())
}

func TestAtLeastOnceDelivery_DoubleConfirm(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 100)

	id := deliverOK(t, d, "/user/target", "msg")
	ok := d.ConfirmDelivery(id)
	assert.True(t, ok)

	// Second confirm should return false
	ok = d.ConfirmDelivery(id)
	assert.False(t, ok)
}

func TestAtLeastOnceDelivery_Snapshot(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 100)

	deliverOK(t, d, "/user/a", "msg1")
	deliverOK(t, d, "/user/b", "msg2")
	d.ConfirmDelivery(1)

	snap := d.GetDeliverySnapshot()
	assert.Equal(t, int64(3), snap.NextDeliveryID)
	assert.Len(t, snap.Pending, 1)
	assert.Equal(t, int64(2), snap.Pending[0].DeliveryID)
	assert.Equal(t, "/user/b", snap.Pending[0].Destination)

	// Restore to a new instance
	d2 := NewAtLeastOnceDelivery(time.Second, 100)
	d2.SetDeliverySnapshot(snap)

	assert.Equal(t, 1, d2.UnconfirmedCount())
	ok := d2.ConfirmDelivery(2)
	assert.True(t, ok)
	assert.Equal(t, 0, d2.UnconfirmedCount())

	// New deliveries should continue from snapshot's NextDeliveryID
	id := deliverOK(t, d2, "/user/c", "msg3")
	assert.Equal(t, int64(3), id)
}

func TestAtLeastOnceDelivery_Redelivery(t *testing.T) {
	d := NewAtLeastOnceDelivery(50*time.Millisecond, 100)

	deliverOK(t, d, "/user/target", "important-msg")

	var mu sync.Mutex
	var redeliveries []int64

	d.StartRedelivery(func(dest string, id int64, msg any) {
		mu.Lock()
		redeliveries = append(redeliveries, id)
		mu.Unlock()
	})
	defer d.StopRedelivery()

	// Wait for at least one redelivery cycle
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(redeliveries) >= 1
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Contains(t, redeliveries, int64(1))
	mu.Unlock()

	// Confirm stops redelivery
	d.ConfirmDelivery(1)
	prevCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(redeliveries)
	}()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, prevCount, len(redeliveries), "no more redeliveries after confirm")
	mu.Unlock()
}

func TestAtLeastOnceDelivery_StopRedelivery(t *testing.T) {
	d := NewAtLeastOnceDelivery(50*time.Millisecond, 100)
	deliverOK(t, d, "/user/target", "msg")

	var count int
	var mu sync.Mutex

	d.StartRedelivery(func(dest string, id int64, msg any) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	time.Sleep(80 * time.Millisecond)
	d.StopRedelivery()

	mu.Lock()
	countAfterStop := count
	mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, countAfterStop, count, "no redeliveries after stop")
	mu.Unlock()
}

func TestAtLeastOnceDelivery_MaxUnconfirmedEnforced(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 3)

	// Three deliveries within the cap succeed.
	_, err := d.Deliver("/user/a", "1")
	require.NoError(t, err)
	_, err = d.Deliver("/user/a", "2")
	require.NoError(t, err)
	_, err = d.Deliver("/user/a", "3")
	require.NoError(t, err)
	assert.Equal(t, 3, d.UnconfirmedCount())

	// The fourth must be rejected and leave state untouched.
	id, err := d.Deliver("/user/a", "4")
	require.ErrorIs(t, err, ErrMaxUnconfirmedMessagesExceeded)
	assert.Equal(t, int64(0), id)
	assert.Equal(t, 3, d.UnconfirmedCount())

	// Confirming one frees a slot — next Deliver succeeds.
	require.True(t, d.ConfirmDelivery(1))
	id, err = d.Deliver("/user/a", "5")
	require.NoError(t, err)
	// Sequence id continues from 4 (rejected delivery did not consume an id).
	assert.Equal(t, int64(4), id)
}

func TestAtLeastOnceDelivery_RedeliveryBurstLimit(t *testing.T) {
	cfg := AtLeastOnceConfig{
		// Long interval so the background ticker does not fire during the
		// in-process invocation below — the assertion measures a single
		// redeliver tick deterministically.
		RedeliverInterval:    1 * time.Hour,
		RedeliveryBurstLimit: 2,
	}
	d := NewAtLeastOnceDeliveryWithConfig(cfg)
	for i := 0; i < 5; i++ {
		_, err := d.Deliver("/user/x", i)
		require.NoError(t, err)
	}

	var (
		mu      sync.Mutex
		perTick int
	)
	d.redeliverPending(func(dest string, id int64, msg any) {
		mu.Lock()
		perTick++
		mu.Unlock()
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, perTick, "burst limit must cap redeliveries to RedeliveryBurstLimit per tick")
	// 5 pending - 2 redelivered (Attempts++ = 2) + 3 untouched (Attempts == 1).
	assert.GreaterOrEqual(t, d.MaxAttempts(), 2)
}

func TestAtLeastOnceDelivery_AttemptsCounted(t *testing.T) {
	d := NewAtLeastOnceDelivery(20*time.Millisecond, 10)
	deliverOK(t, d, "/user/x", "m")

	d.StartRedelivery(func(dest string, id int64, msg any) {})
	defer d.StopRedelivery()

	require.Eventually(t, func() bool {
		return d.MaxAttempts() >= 3
	}, time.Second, 10*time.Millisecond)

	// Snapshot must capture the elevated Attempts count.
	snap := d.GetDeliverySnapshot()
	require.Len(t, snap.Pending, 1)
	assert.GreaterOrEqual(t, snap.Pending[0].Attempts, 3)
}

func TestSetDefaultAtLeastOnceConfig_PartialOverride(t *testing.T) {
	prev := DefaultAtLeastOnceConfig()
	t.Cleanup(func() { SetDefaultAtLeastOnceConfig(prev) })

	SetDefaultAtLeastOnceConfig(AtLeastOnceConfig{RedeliverInterval: 7 * time.Second})
	got := DefaultAtLeastOnceConfig()
	assert.Equal(t, 7*time.Second, got.RedeliverInterval)
	// Other fields keep their previous values.
	assert.Equal(t, prev.MaxUnconfirmedMessages, got.MaxUnconfirmedMessages)
	assert.Equal(t, prev.RedeliveryBurstLimit, got.RedeliveryBurstLimit)
	assert.Equal(t, prev.WarnAfterNumberOfUnconfirmedAttempts, got.WarnAfterNumberOfUnconfirmedAttempts)
}

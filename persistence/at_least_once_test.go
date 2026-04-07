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

func TestAtLeastOnceDelivery_DeliverAndConfirm(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 100)

	id1 := d.Deliver("/user/target", "msg1")
	id2 := d.Deliver("/user/target", "msg2")

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

	id := d.Deliver("/user/target", "msg")
	ok := d.ConfirmDelivery(id)
	assert.True(t, ok)

	// Second confirm should return false
	ok = d.ConfirmDelivery(id)
	assert.False(t, ok)
}

func TestAtLeastOnceDelivery_Snapshot(t *testing.T) {
	d := NewAtLeastOnceDelivery(time.Second, 100)

	d.Deliver("/user/a", "msg1")
	d.Deliver("/user/b", "msg2")
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
	id := d2.Deliver("/user/c", "msg3")
	assert.Equal(t, int64(3), id)
}

func TestAtLeastOnceDelivery_Redelivery(t *testing.T) {
	d := NewAtLeastOnceDelivery(50*time.Millisecond, 100)

	d.Deliver("/user/target", "important-msg")

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
	d.Deliver("/user/target", "msg")

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

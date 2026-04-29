//go:build integration
// +build integration

/*
 * aald_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyReceiver simulates an ack-requiring downstream that drops the
// first dropTimes deliveries of every deliveryID before finally
// acknowledging.  Ack is delivered by calling ConfirmDelivery on the
// supplied AtLeastOnceDelivery, mirroring how a real persistent actor
// would relay the receiver's reply back into its own state.
type flakyReceiver struct {
	mu        sync.Mutex
	dropTimes int
	seen      map[int64]int
	confirmed map[int64]bool
	d         *persistence.AtLeastOnceDelivery
}

func newFlakyReceiver(d *persistence.AtLeastOnceDelivery, dropTimes int) *flakyReceiver {
	return &flakyReceiver{
		dropTimes: dropTimes,
		seen:      make(map[int64]int),
		confirmed: make(map[int64]bool),
		d:         d,
	}
}

// onSend models the receiver's network step.  Returns true when the
// delivery is "successfully" acked, false when dropped.
func (r *flakyReceiver) onSend(_ string, deliveryID int64, _ any) bool {
	r.mu.Lock()
	r.seen[deliveryID]++
	attempts := r.seen[deliveryID]
	r.mu.Unlock()

	if attempts <= r.dropTimes {
		return false
	}
	r.mu.Lock()
	if r.confirmed[deliveryID] {
		r.mu.Unlock()
		return true
	}
	r.confirmed[deliveryID] = true
	r.mu.Unlock()

	r.d.ConfirmDelivery(deliveryID)
	return true
}

func (r *flakyReceiver) confirmedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.confirmed)
}

// TestIntegration_AALD_ReliableSendWithFlakyReceiver verifies the round
// trip: producer schedules deliveries, scheduler's redelivery loop keeps
// retrying through dropped packets, receiver eventually acks every id,
// and the unconfirmed set drains to zero.
func TestIntegration_AALD_ReliableSendWithFlakyReceiver(t *testing.T) {
	d := persistence.NewAtLeastOnceDeliveryWithConfig(persistence.AtLeastOnceConfig{
		RedeliverInterval:                    20 * time.Millisecond,
		RedeliveryBurstLimit:                 100,
		WarnAfterNumberOfUnconfirmedAttempts: 50, // do not warn during the success-path test
		MaxUnconfirmedMessages:               1000,
	})

	receiver := newFlakyReceiver(d, 2) // drop first 2 attempts of each id
	var redeliveries int64
	scheduler := persistence.NewScheduler(d,
		func(dest string, id int64, msg any) {
			atomic.AddInt64(&redeliveries, 1)
			receiver.onSend(dest, id, msg)
		},
		nil,
	)

	const total = 10
	for i := 0; i < total; i++ {
		_, err := d.Deliver("/user/sink", i)
		require.NoError(t, err)
		// First "wire send" happens at Deliver-time in real systems; mirror that here.
		receiver.onSend("/user/sink", int64(i+1), i)
	}

	scheduler.Start()
	defer scheduler.Stop()

	require.Eventually(t, func() bool {
		return d.UnconfirmedCount() == 0 && receiver.confirmedCount() == total
	}, 3*time.Second, 20*time.Millisecond, "all deliveries must eventually drain")

	assert.Equal(t, 0, d.UnconfirmedCount())
	assert.Equal(t, total, receiver.confirmedCount())
	assert.GreaterOrEqual(t, atomic.LoadInt64(&redeliveries), int64(total),
		"redelivery loop should have fired at least once per pending message")
}

// TestIntegration_AALD_AckSurvivesRestart verifies the persistence
// guarantee: after a snapshot/restart cycle the new AtLeastOnceDelivery
// instance still tracks the in-flight ids and a late ack drains them.
func TestIntegration_AALD_AckSurvivesRestart(t *testing.T) {
	store := persistence.NewInMemorySnapshotStore()
	const persistenceID = "aald-restart-it"

	// --- Producer #1: enqueues 5 messages, none acked, snapshots state.
	d1 := persistence.NewAtLeastOnceDeliveryWithConfig(persistence.AtLeastOnceConfig{
		RedeliverInterval:                    1 * time.Hour, // freeze background loop for determinism
		RedeliveryBurstLimit:                 10,
		WarnAfterNumberOfUnconfirmedAttempts: 5,
		MaxUnconfirmedMessages:               100,
	})
	for i := 0; i < 5; i++ {
		_, err := d1.Deliver("/user/sink", i)
		require.NoError(t, err)
	}
	require.Equal(t, 5, d1.UnconfirmedCount())
	require.NoError(t, persistence.SaveAtLeastOnceSnapshot(
		context.Background(), store, persistenceID, 1, d1))

	// --- "Restart": construct a fresh tracker, hydrate from snapshot.
	d2 := persistence.NewAtLeastOnceDeliveryWithConfig(persistence.AtLeastOnceConfig{
		RedeliverInterval:                    1 * time.Hour,
		RedeliveryBurstLimit:                 10,
		WarnAfterNumberOfUnconfirmedAttempts: 5,
		MaxUnconfirmedMessages:               100,
	})
	_, err := persistence.LoadAtLeastOnceSnapshot(
		context.Background(), store, persistenceID, d2)
	require.NoError(t, err)
	require.Equal(t, 5, d2.UnconfirmedCount(),
		"restored tracker must hold the same pending count as the source")

	// Late acks now drain the restored set.
	for i := int64(1); i <= 5; i++ {
		assert.True(t, d2.ConfirmDelivery(i),
			"delivery %d must be confirmable after snapshot restore", i)
	}
	assert.Equal(t, 0, d2.UnconfirmedCount())

	// New deliveries continue from the persisted NextDeliveryID.
	id, err := d2.Deliver("/user/sink", "post-restart")
	require.NoError(t, err)
	assert.Equal(t, int64(6), id)
}

// TestIntegration_AALD_WarnCallbackFiresAfterThreshold verifies that the
// warn callback fires exactly once per delivery once Attempts reaches
// WarnAfterNumberOfUnconfirmedAttempts.
func TestIntegration_AALD_WarnCallbackFiresAfterThreshold(t *testing.T) {
	d := persistence.NewAtLeastOnceDeliveryWithConfig(persistence.AtLeastOnceConfig{
		RedeliverInterval:                    10 * time.Millisecond,
		RedeliveryBurstLimit:                 10,
		WarnAfterNumberOfUnconfirmedAttempts: 3,
		MaxUnconfirmedMessages:               10,
	})

	var (
		warnsMu sync.Mutex
		warns   = make(map[int64]int)
	)
	scheduler := persistence.NewScheduler(d,
		func(dest string, id int64, msg any) {
			// black-hole receiver: never confirms, so attempts climb forever.
		},
		func(dest string, id int64, attempts int) {
			warnsMu.Lock()
			warns[id]++
			warnsMu.Unlock()
		},
	)

	for i := 0; i < 3; i++ {
		_, err := d.Deliver("/user/sink", i)
		require.NoError(t, err)
	}

	scheduler.Start()
	defer scheduler.Stop()

	require.Eventually(t, func() bool {
		warnsMu.Lock()
		defer warnsMu.Unlock()
		return len(warns) == 3
	}, 2*time.Second, 10*time.Millisecond, "warn callback must fire once for each pending delivery")

	// Let several more redelivery ticks elapse — the warn count must
	// stay at 1 per delivery (once-only contract).
	time.Sleep(80 * time.Millisecond)
	warnsMu.Lock()
	defer warnsMu.Unlock()
	for id, count := range warns {
		assert.Equal(t, 1, count, "delivery %d warn callback fired %d times; expected exactly 1", id, count)
	}
}

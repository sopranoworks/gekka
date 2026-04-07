/*
 * at_least_once.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"sync"
	"time"
)

// AtLeastOnceDelivery provides reliable message delivery with redelivery
// of unconfirmed messages. It can be embedded in a persistent actor to
// automatically redeliver messages that haven't been confirmed within
// a configurable interval.
//
// Usage:
//  1. Call Deliver(destination, deliveryId, msg) to send a message
//  2. The recipient confirms by calling back ConfirmDelivery(deliveryId)
//  3. Unconfirmed messages are redelivered every RedeliverInterval
//  4. Use GetDeliverySnapshot/SetDeliverySnapshot for persistence across restarts
type AtLeastOnceDelivery struct {
	mu                     sync.Mutex
	nextDeliveryID         int64
	unconfirmed            map[int64]*pendingDelivery
	redeliverInterval      time.Duration
	maxUnconfirmedMessages int
	stopCh                 chan struct{}
	running                bool
}

type pendingDelivery struct {
	DeliveryID  int64
	Destination string
	Message     any
	Timestamp   time.Time
	Attempts    int
}

// DeliverySnapshot captures the state of pending deliveries for
// persistence across actor restarts.
type DeliverySnapshot struct {
	NextDeliveryID int64
	Pending        []PendingDeliveryEntry
}

// PendingDeliveryEntry is a single unconfirmed delivery in a snapshot.
type PendingDeliveryEntry struct {
	DeliveryID  int64
	Destination string
	Message     any
	Attempts    int
}

// NewAtLeastOnceDelivery creates a new delivery tracker with the given
// redelivery interval and maximum unconfirmed message limit.
func NewAtLeastOnceDelivery(redeliverInterval time.Duration, maxUnconfirmed int) *AtLeastOnceDelivery {
	if redeliverInterval <= 0 {
		redeliverInterval = 5 * time.Second
	}
	if maxUnconfirmed <= 0 {
		maxUnconfirmed = 1000
	}
	return &AtLeastOnceDelivery{
		nextDeliveryID:         1,
		unconfirmed:            make(map[int64]*pendingDelivery),
		redeliverInterval:      redeliverInterval,
		maxUnconfirmedMessages: maxUnconfirmed,
		stopCh:                 make(chan struct{}),
	}
}

// Deliver registers a new message for delivery. Returns the deliveryId
// and the message (with deliveryId injected via messageFactory).
// The caller is responsible for actually sending the message.
func (d *AtLeastOnceDelivery) Deliver(destination string, msg any) (deliveryID int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	id := d.nextDeliveryID
	d.nextDeliveryID++

	d.unconfirmed[id] = &pendingDelivery{
		DeliveryID:  id,
		Destination: destination,
		Message:     msg,
		Timestamp:   time.Now(),
		Attempts:    1,
	}

	return id
}

// ConfirmDelivery marks a delivery as confirmed (successfully received).
// Returns true if the deliveryId was pending, false if already confirmed
// or unknown.
func (d *AtLeastOnceDelivery) ConfirmDelivery(deliveryID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.unconfirmed[deliveryID]; ok {
		delete(d.unconfirmed, deliveryID)
		return true
	}
	return false
}

// UnconfirmedCount returns the number of unconfirmed deliveries.
func (d *AtLeastOnceDelivery) UnconfirmedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.unconfirmed)
}

// GetDeliverySnapshot returns the current state of all pending deliveries
// for persistence. Call this before saving a snapshot.
func (d *AtLeastOnceDelivery) GetDeliverySnapshot() DeliverySnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	snap := DeliverySnapshot{
		NextDeliveryID: d.nextDeliveryID,
		Pending:        make([]PendingDeliveryEntry, 0, len(d.unconfirmed)),
	}

	for _, pd := range d.unconfirmed {
		snap.Pending = append(snap.Pending, PendingDeliveryEntry{
			DeliveryID:  pd.DeliveryID,
			Destination: pd.Destination,
			Message:     pd.Message,
			Attempts:    pd.Attempts,
		})
	}

	return snap
}

// SetDeliverySnapshot restores delivery state from a persisted snapshot.
// Call this during recovery.
func (d *AtLeastOnceDelivery) SetDeliverySnapshot(snap DeliverySnapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextDeliveryID = snap.NextDeliveryID
	d.unconfirmed = make(map[int64]*pendingDelivery, len(snap.Pending))

	for _, entry := range snap.Pending {
		d.unconfirmed[entry.DeliveryID] = &pendingDelivery{
			DeliveryID:  entry.DeliveryID,
			Destination: entry.Destination,
			Message:     entry.Message,
			Timestamp:   time.Now(),
			Attempts:    entry.Attempts,
		}
	}
}

// StartRedelivery begins a background goroutine that periodically checks
// for unconfirmed messages and calls the redeliver function for each one.
// The redeliver function should re-send the message to the destination.
func (d *AtLeastOnceDelivery) StartRedelivery(redeliver func(destination string, deliveryID int64, msg any)) {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return
	}
	d.running = true
	d.mu.Unlock()

	go func() {
		ticker := time.NewTicker(d.redeliverInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.stopCh:
				return
			case <-ticker.C:
				d.redeliverPending(redeliver)
			}
		}
	}()
}

// StopRedelivery stops the background redelivery goroutine.
func (d *AtLeastOnceDelivery) StopRedelivery() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		close(d.stopCh)
		d.running = false
		d.stopCh = make(chan struct{})
	}
}

func (d *AtLeastOnceDelivery) redeliverPending(redeliver func(string, int64, any)) {
	d.mu.Lock()
	var pending []*pendingDelivery
	for _, pd := range d.unconfirmed {
		pending = append(pending, pd)
		pd.Attempts++
	}
	d.mu.Unlock()

	for _, pd := range pending {
		redeliver(pd.Destination, pd.DeliveryID, pd.Message)
	}
}

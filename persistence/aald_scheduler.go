/*
 * aald_scheduler.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

// RedeliverFunc is invoked for each unconfirmed delivery the scheduler
// re-fires.  It must perform the actual side-effecting send.
type RedeliverFunc func(destination string, deliveryID int64, msg any)

// WarnFunc is invoked at most once per delivery, the first time its
// Attempts counter reaches WarnAfterNumberOfUnconfirmedAttempts.  It is
// the persistence-actor's hook for logging the
// "still unconfirmed after N attempts" warning that Pekko produces.
type WarnFunc func(destination string, deliveryID int64, attempts int)

// SetWarnCallback registers (or replaces) the warn callback for this
// delivery tracker.  Pass nil to clear it.  The callback is invoked from
// the redelivery goroutine; implementations must be cheap and
// concurrency-safe.
func (d *AtLeastOnceDelivery) SetWarnCallback(fn WarnFunc) {
	d.mu.Lock()
	d.warn = fn
	d.mu.Unlock()
}

// Scheduler wires an AtLeastOnceDelivery instance to its redelivery and
// warn callbacks so a persistent actor can start/stop the redelivery loop
// with a single call.  It is intentionally tiny — the heavy lifting lives
// on AtLeastOnceDelivery itself; Scheduler is the integration seam.
type Scheduler struct {
	d         *AtLeastOnceDelivery
	redeliver RedeliverFunc
	warn      WarnFunc
}

// NewScheduler binds the given AtLeastOnceDelivery to the redeliver and
// (optional) warn callbacks.  Either callback may be nil; a nil
// redeliver disables redelivery (Start becomes a no-op) and a nil warn
// silently suppresses the warn-after-attempts hook.
func NewScheduler(d *AtLeastOnceDelivery, redeliver RedeliverFunc, warn WarnFunc) *Scheduler {
	return &Scheduler{d: d, redeliver: redeliver, warn: warn}
}

// Start begins the redelivery loop.  The first tick fires after one
// RedeliverInterval; cap each tick at RedeliveryBurstLimit.  Calling
// Start when the scheduler is already running is a no-op.
func (s *Scheduler) Start() {
	if s.redeliver == nil {
		return
	}
	s.d.SetWarnCallback(s.warn)
	s.d.StartRedelivery(s.redeliver)
}

// Stop halts the background loop.  Pending deliveries remain in memory
// and a subsequent Start resumes the redelivery cycle.
func (s *Scheduler) Stop() {
	s.d.StopRedelivery()
}

// Delivery exposes the underlying AtLeastOnceDelivery so callers can
// invoke Deliver / ConfirmDelivery / snapshot helpers without holding a
// separate reference.
func (s *Scheduler) Delivery() *AtLeastOnceDelivery {
	return s.d
}

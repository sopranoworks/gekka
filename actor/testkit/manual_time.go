/*
 * manual_time.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"sync"
	"time"
)

// ManualTime provides deterministic time control for testing actors that use
// timers.  Instead of relying on real wall-clock time, test code advances time
// explicitly via [ManualTime.Advance].
//
// Scheduled tasks fire in order when time is advanced past their deadline.
type ManualTime struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*manualTimer
	counter int
}

type manualTimer struct {
	id       int
	deadline time.Time
	callback func()
	key      any
	active   bool
}

// NewManualTime creates a [ManualTime] starting at the given time.
// If zero, defaults to a fixed epoch.
func NewManualTime(start ...time.Time) *ManualTime {
	t := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if len(start) > 0 {
		t = start[0]
	}
	return &ManualTime{now: t}
}

// Now returns the current simulated time.
func (mt *ManualTime) Now() time.Time {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.now
}

// Advance moves the clock forward by d and fires any timers whose deadline
// falls within the new time window.  Timers fire in chronological order.
func (mt *ManualTime) Advance(d time.Duration) {
	mt.mu.Lock()
	newNow := mt.now.Add(d)
	mt.now = newNow

	// Collect and sort timers that should fire
	var toFire []*manualTimer
	for _, t := range mt.timers {
		if t.active && !t.deadline.After(newNow) {
			toFire = append(toFire, t)
		}
	}

	// Sort by deadline
	for i := 0; i < len(toFire); i++ {
		for j := i + 1; j < len(toFire); j++ {
			if toFire[j].deadline.Before(toFire[i].deadline) {
				toFire[i], toFire[j] = toFire[j], toFire[i]
			}
		}
	}
	mt.mu.Unlock()

	// Fire outside lock to prevent deadlocks
	for _, t := range toFire {
		mt.mu.Lock()
		if t.active {
			t.active = false
			mt.mu.Unlock()
			t.callback()
		} else {
			mt.mu.Unlock()
		}
	}
}

// Schedule registers a callback to fire after delay from the current time.
// Returns a cancel function.
func (mt *ManualTime) Schedule(key any, delay time.Duration, callback func()) func() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.counter++
	t := &manualTimer{
		id:       mt.counter,
		deadline: mt.now.Add(delay),
		callback: callback,
		key:      key,
		active:   true,
	}
	mt.timers = append(mt.timers, t)

	return func() {
		mt.mu.Lock()
		defer mt.mu.Unlock()
		t.active = false
	}
}

// TimerCount returns the number of active (not yet fired or cancelled) timers.
func (mt *ManualTime) TimerCount() int {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	count := 0
	for _, t := range mt.timers {
		if t.active {
			count++
		}
	}
	return count
}

// CancelAll cancels all active timers.
func (mt *ManualTime) CancelAll() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	for _, t := range mt.timers {
		t.active = false
	}
}

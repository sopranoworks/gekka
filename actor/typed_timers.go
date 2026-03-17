/*
 * typed_timers.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"time"
)

// TimerScheduler provides time-based message scheduling for a typed actor.
// All active timers are automatically cancelled when the actor stops.
//
// Timer keys uniquely identify each timer within an actor. Starting a new timer
// with a key that is already active cancels the previous timer first.
// Keys must be comparable (e.g. string, int, or a comparable struct).
type TimerScheduler[T any] interface {
	// StartTimerWithFixedDelay schedules msg to be sent to the actor repeatedly
	// with a fixed delay between each delivery. Any existing timer registered
	// under key is cancelled first.
	StartTimerWithFixedDelay(key any, msg T, delay time.Duration)

	// StartSingleTimer schedules msg to be sent to the actor once after delay.
	// Any existing timer registered under key is cancelled first.
	StartSingleTimer(key any, msg T, delay time.Duration)

	// Cancel cancels the timer registered under key. No-op if key is not active.
	Cancel(key any)

	// IsTimerActive reports whether a timer registered under key is currently scheduled.
	IsTimerActive(key any) bool
}

type timerEntry struct {
	cancel chan struct{} // closed to stop this specific timer's goroutine
}

// timerScheduler is the concrete, goroutine-safe implementation of TimerScheduler[T].
type timerScheduler[T any] struct {
	mu     sync.Mutex
	timers map[any]*timerEntry
	self   Ref
	stopCh chan struct{} // closed once when the owning actor stops
}

func newTimerScheduler[T any](self Ref) *timerScheduler[T] {
	return &timerScheduler[T]{
		timers: make(map[any]*timerEntry),
		self:   self,
		stopCh: make(chan struct{}),
	}
}

// cancelLocked cancels and removes the timer for key. Caller must hold mu.
func (s *timerScheduler[T]) cancelLocked(key any) {
	if entry, ok := s.timers[key]; ok {
		close(entry.cancel)
		delete(s.timers, key)
	}
}

func (s *timerScheduler[T]) Cancel(key any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelLocked(key)
}

func (s *timerScheduler[T]) IsTimerActive(key any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.timers[key]
	return ok
}

func (s *timerScheduler[T]) StartSingleTimer(key any, msg T, delay time.Duration) {
	s.mu.Lock()
	s.cancelLocked(key)
	cancelCh := make(chan struct{})
	entry := &timerEntry{cancel: cancelCh}
	s.timers[key] = entry
	self := s.self
	stopCh := s.stopCh
	s.mu.Unlock()

	go func() {
		select {
		case <-time.After(delay):
			self.Tell(msg)
			// Deregister only if this exact entry is still current.
			s.mu.Lock()
			if e, ok := s.timers[key]; ok && e == entry {
				delete(s.timers, key)
			}
			s.mu.Unlock()
		case <-cancelCh:
		case <-stopCh:
		}
	}()
}

func (s *timerScheduler[T]) StartTimerWithFixedDelay(key any, msg T, delay time.Duration) {
	s.mu.Lock()
	s.cancelLocked(key)
	cancelCh := make(chan struct{})
	s.timers[key] = &timerEntry{cancel: cancelCh}
	self := s.self
	stopCh := s.stopCh
	s.mu.Unlock()

	go func() {
		for {
			select {
			case <-time.After(delay):
				self.Tell(msg)
			case <-cancelCh:
				return
			case <-stopCh:
				return
			}
		}
	}()
}

// cancelAll stops all active timers. Called by typedActor.PostStop.
func (s *timerScheduler[T]) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Closing stopCh unblocks all goroutines atomically.
	select {
	case <-s.stopCh:
		// already stopped — nothing to do
	default:
		close(s.stopCh)
	}
	clear(s.timers)
}

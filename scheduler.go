/*
 * scheduler.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"sync"
	"time"
)

// Cancellable is a handle returned by Scheduler methods that can be used to
// cancel a scheduled task before it executes (or to stop a repeating task).
// Cancel returns true if the task was successfully cancelled, false if it had
// already fired or been cancelled previously.
type Cancellable interface {
	Cancel() bool
}

// Scheduler provides system-level time-based task scheduling.
// Tasks are plain functions run in their own goroutine; they are not associated
// with any actor mailbox.  Use actor.TimerScheduler (via TypedContext.Timers())
// when you need timer messages delivered to a specific actor.
//
// All tasks scheduled on a system's Scheduler are cancelled when the system
// is terminated.
type Scheduler interface {
	// ScheduleOnce runs f once after delay.  The returned Cancellable can stop
	// the task before it fires; it has no effect once f has started.
	ScheduleOnce(delay time.Duration, f func()) Cancellable

	// ScheduleWithFixedDelay runs f repeatedly.  The first execution happens
	// after initialDelay; subsequent executions happen with a fixed delay
	// between the end of one execution and the start of the next.
	// The task continues until Cancel is called on the returned handle or the
	// owning ActorSystem is terminated.
	ScheduleWithFixedDelay(initialDelay, delay time.Duration, f func()) Cancellable
}

// ----------------------------------------------------------------------------
// onceCancellable — wraps time.Timer for single-fire tasks
// ----------------------------------------------------------------------------

type onceCancellable struct {
	timer *time.Timer
}

func (c *onceCancellable) Cancel() bool {
	return c.timer.Stop()
}

// ----------------------------------------------------------------------------
// tickCancellable — manages a repeating goroutine
// ----------------------------------------------------------------------------

type tickCancellable struct {
	once   sync.Once
	stopCh chan struct{}
}

func (c *tickCancellable) Cancel() bool {
	cancelled := false
	c.once.Do(func() {
		close(c.stopCh)
		cancelled = true
	})
	return cancelled
}

// ----------------------------------------------------------------------------
// systemScheduler — concrete Scheduler implementation
// ----------------------------------------------------------------------------

// systemScheduler runs scheduled tasks and tracks all active handles so they
// can be cancelled when the owning system is terminated.
type systemScheduler struct {
	mu      sync.Mutex
	tasks   map[*tickCancellable]struct{} // only repeating tasks need tracking
	stopped bool
	stopCh  chan struct{} // closed on Terminate
}

func newSystemScheduler() *systemScheduler {
	return &systemScheduler{
		tasks:  make(map[*tickCancellable]struct{}),
		stopCh: make(chan struct{}),
	}
}

func (s *systemScheduler) ScheduleOnce(delay time.Duration, f func()) Cancellable {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return &onceCancellable{timer: time.NewTimer(0)} // already-expired, Cancel() returns false
	}
	s.mu.Unlock()

	t := time.AfterFunc(delay, f)
	return &onceCancellable{timer: t}
}

func (s *systemScheduler) ScheduleWithFixedDelay(initialDelay, delay time.Duration, f func()) Cancellable {
	c := &tickCancellable{stopCh: make(chan struct{})}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		close(c.stopCh) // already cancelled
		return c
	}
	s.tasks[c] = struct{}{}
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.tasks, c)
			s.mu.Unlock()
		}()

		// Initial delay.
		select {
		case <-time.After(initialDelay):
		case <-c.stopCh:
			return
		case <-s.stopCh:
			return
		}

		for {
			f()
			select {
			case <-time.After(delay):
			case <-c.stopCh:
				return
			case <-s.stopCh:
				return
			}
		}
	}()

	return c
}

// terminate cancels all active repeating tasks and prevents new tasks from
// being scheduled. Called by ActorSystem.Terminate (or equivalent shutdown).
func (s *systemScheduler) terminate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	close(s.stopCh)
	clear(s.tasks)
}

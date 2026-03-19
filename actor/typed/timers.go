/*
 * typed_timers.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TimerScheduler allows for scheduling messages to be sent to the actor's
// self reference after a delay or periodically.
type TimerScheduler[T any] interface {
	// StartSingleTimer schedules a message to be sent once after the delay.
	StartSingleTimer(key any, msg T, delay time.Duration)

	// StartPeriodicTimer schedules a message to be sent periodically.
	StartPeriodicTimer(key any, msg T, interval time.Duration)

	// IsTimerActive returns true if a timer with the given key is active.
	IsTimerActive(key any) bool

	// Cancel stops and removes the timer with the given key.
	Cancel(key any)

	// CancelAll stops and removes all active timers.
	CancelAll()
}

// We re-use the exported TimerSchedulerImpl from package actor.
type timerScheduler[T any] struct {
	*actor.TimerSchedulerImpl[T]
}

func newTimerScheduler[T any](self actor.Ref) *timerScheduler[T] {
	return &timerScheduler[T]{
		TimerSchedulerImpl: actor.NewTimerScheduler[T](self),
	}
}

func (s *timerScheduler[T]) CancelAll() {
	s.TimerSchedulerImpl.CancelAll()
}

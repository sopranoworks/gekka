/*
 * timers.go
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

type TimerSchedulerImpl[T any] struct {
	self   Ref
	timers map[any]*time.Timer
	mu     sync.Mutex
	stopCh chan struct{}
}

func NewTimerScheduler[T any](self Ref) *TimerSchedulerImpl[T] {
	return &TimerSchedulerImpl[T]{
		self:   self,
		timers: make(map[any]*time.Timer),
		stopCh: make(chan struct{}),
	}
}

func (s *TimerSchedulerImpl[T]) StartSingleTimer(key any, msg T, delay time.Duration) {
	s.Cancel(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	timer := time.AfterFunc(delay, func() {
		s.mu.Lock()
		delete(s.timers, key)
		s.mu.Unlock()
		
		// Send message to self.
		s.self.Tell(msg)
	})
	s.timers[key] = timer
}

func (s *TimerSchedulerImpl[T]) StartPeriodicTimer(key any, msg T, interval time.Duration) {
	s.Cancel(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	var runner func()
	runner = func() {
		s.mu.Lock()
		_, ok := s.timers[key]
		if !ok {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		s.self.Tell(msg)

		s.mu.Lock()
		s.timers[key] = time.AfterFunc(interval, runner)
		s.mu.Unlock()
	}

	s.timers[key] = time.AfterFunc(interval, runner)
}

func (s *TimerSchedulerImpl[T]) IsTimerActive(key any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.timers[key]
	return ok
}

func (s *TimerSchedulerImpl[T]) Cancel(key any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.timers[key]; ok {
		t.Stop()
		delete(s.timers, key)
	}
}

func (s *TimerSchedulerImpl[T]) CancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, t := range s.timers {
		t.Stop()
		delete(s.timers, k)
	}
}

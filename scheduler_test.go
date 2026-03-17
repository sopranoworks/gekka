/*
 * scheduler_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"sync/atomic"
	"testing"
	"time"
)

// newTestScheduler creates a fresh scheduler for unit tests.
func newTestScheduler() *systemScheduler {
	return newSystemScheduler()
}

// ── ScheduleOnce ────────────────────────────────────────────────────────────

func TestScheduleOnce_ExecutesAfterDelay(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	done := make(chan struct{})
	s.ScheduleOnce(30*time.Millisecond, func() { close(done) })

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ScheduleOnce did not execute within timeout")
	}
}

func TestScheduleOnce_ExecutesOnlyOnce(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	var count atomic.Int32
	s.ScheduleOnce(20*time.Millisecond, func() { count.Add(1) })

	time.Sleep(200 * time.Millisecond)
	if n := count.Load(); n != 1 {
		t.Errorf("expected exactly 1 execution, got %d", n)
	}
}

func TestScheduleOnce_CancelBeforeFire(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	var fired atomic.Bool
	c := s.ScheduleOnce(200*time.Millisecond, func() { fired.Store(true) })

	cancelled := c.Cancel()
	if !cancelled {
		t.Error("Cancel() should return true when task is still pending")
	}

	time.Sleep(400 * time.Millisecond)
	if fired.Load() {
		t.Error("task fired after Cancel()")
	}
}

func TestScheduleOnce_CancelAfterFire(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	done := make(chan struct{})
	c := s.ScheduleOnce(20*time.Millisecond, func() { close(done) })

	<-done // wait for it to fire
	time.Sleep(10 * time.Millisecond)

	// Cancel after firing should return false (timer already expired).
	if c.Cancel() {
		t.Error("Cancel() should return false after task has already fired")
	}
}

// ── ScheduleWithFixedDelay ─────────────────────────────────────────────────

func TestScheduleWithFixedDelay_ExecutesPeriodically(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	var count atomic.Int32
	c := s.ScheduleWithFixedDelay(0, 20*time.Millisecond, func() { count.Add(1) })
	defer c.Cancel()

	deadline := time.After(500 * time.Millisecond)
	for count.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("fixed-delay task executed only %d/3 times", count.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestScheduleWithFixedDelay_InitialDelay(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	var count atomic.Int32
	initialDelay := 100 * time.Millisecond
	c := s.ScheduleWithFixedDelay(initialDelay, 10*time.Millisecond, func() { count.Add(1) })
	defer c.Cancel()

	// Should not have fired yet.
	time.Sleep(50 * time.Millisecond)
	if n := count.Load(); n > 0 {
		t.Errorf("task fired before initialDelay elapsed (count=%d)", n)
	}

	// Should fire soon after initial delay passes.
	time.Sleep(200 * time.Millisecond)
	if n := count.Load(); n == 0 {
		t.Error("task did not fire after initialDelay elapsed")
	}
}

func TestScheduleWithFixedDelay_Cancel(t *testing.T) {
	s := newTestScheduler()
	defer s.terminate()

	var count atomic.Int32
	c := s.ScheduleWithFixedDelay(0, 15*time.Millisecond, func() { count.Add(1) })

	// Wait for at least one execution.
	deadline := time.After(300 * time.Millisecond)
	for count.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("task never executed before cancel")
		case <-time.After(5 * time.Millisecond):
		}
	}

	if !c.Cancel() {
		t.Error("Cancel() should return true on first call")
	}
	if c.Cancel() {
		t.Error("Cancel() should return false on subsequent calls")
	}

	snapshot := count.Load()
	time.Sleep(100 * time.Millisecond)
	if after := count.Load(); after > snapshot+1 {
		// Allow one in-flight execution.
		t.Errorf("task continued executing after Cancel(): count grew from %d to %d", snapshot, after)
	}
}

// ── ActorSystem.Scheduler() ────────────────────────────────────────────────

func TestActorSystem_Scheduler_NotNil(t *testing.T) {
	sys, err := NewActorSystem("test-scheduler")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	if sys.Scheduler() == nil {
		t.Error("ActorSystem.Scheduler() returned nil")
	}
}

func TestActorSystem_Scheduler_SharedInstance(t *testing.T) {
	sys, err := NewActorSystem("test-shared")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	if sys.Scheduler() != sys.Scheduler() {
		t.Error("Scheduler() should return the same instance on every call")
	}
}

// ── System termination cancels all tasks ──────────────────────────────────

func TestScheduler_Terminate_CancelsAllTasks(t *testing.T) {
	s := newTestScheduler()

	var onceCount, repeatCount atomic.Int32

	// Long-delay single task — should never fire.
	s.ScheduleOnce(10*time.Second, func() { onceCount.Add(1) })

	// Repeating task — should stop after termination.
	c := s.ScheduleWithFixedDelay(0, 10*time.Millisecond, func() { repeatCount.Add(1) })
	defer c.Cancel()

	// Let the repeating task fire a couple of times.
	time.Sleep(50 * time.Millisecond)

	s.terminate()
	snapshot := repeatCount.Load()

	// No more executions should happen.
	time.Sleep(100 * time.Millisecond)
	if after := repeatCount.Load(); after > snapshot+1 {
		t.Errorf("repeating task continued after terminate: %d → %d", snapshot, after)
	}
	if onceCount.Load() > 0 {
		t.Error("single task fired after terminate")
	}
}

func TestScheduler_Terminate_PreventsNewTasks(t *testing.T) {
	s := newTestScheduler()
	s.terminate()

	var fired atomic.Bool
	s.ScheduleOnce(5*time.Millisecond, func() { fired.Store(true) })
	s.ScheduleWithFixedDelay(0, 5*time.Millisecond, func() { fired.Store(true) })

	time.Sleep(50 * time.Millisecond)
	if fired.Load() {
		t.Error("task fired on a terminated scheduler")
	}
}

func TestActorSystem_ContextCancel_TerminatesScheduler(t *testing.T) {
	las, ok := func() (*localActorSystem, bool) {
		sys, _ := NewActorSystem("ctx-cancel-test")
		ls, ok := sys.(*localActorSystem)
		return ls, ok
	}()
	if !ok {
		t.Skip("NewActorSystem did not return *localActorSystem")
	}

	var count atomic.Int32
	las.sched.ScheduleWithFixedDelay(0, 10*time.Millisecond, func() { count.Add(1) })

	time.Sleep(50 * time.Millisecond)
	las.cancel() // triggers context Done → scheduler.terminate()
	time.Sleep(20 * time.Millisecond)

	snapshot := count.Load()
	time.Sleep(80 * time.Millisecond)
	if after := count.Load(); after > snapshot+1 {
		t.Errorf("task continued after context cancel: %d → %d", snapshot, after)
	}
}

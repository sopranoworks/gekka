/*
 * manual_time_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"testing"
	"time"
)

func TestManualTime_ScheduleAndAdvance(t *testing.T) {
	mt := NewManualTime()
	var fired []string

	mt.Schedule("a", 5*time.Second, func() { fired = append(fired, "a") })
	mt.Schedule("b", 3*time.Second, func() { fired = append(fired, "b") })

	if mt.TimerCount() != 2 {
		t.Errorf("expected 2 timers, got %d", mt.TimerCount())
	}

	// Advance 3s: only "b" should fire
	mt.Advance(3 * time.Second)
	if len(fired) != 1 || fired[0] != "b" {
		t.Errorf("after 3s: fired = %v, want [b]", fired)
	}
	if mt.TimerCount() != 1 {
		t.Errorf("expected 1 active timer, got %d", mt.TimerCount())
	}

	// Advance 3 more seconds (total 6s): "a" should fire
	mt.Advance(3 * time.Second)
	if len(fired) != 2 || fired[1] != "a" {
		t.Errorf("after 6s: fired = %v, want [b, a]", fired)
	}
	if mt.TimerCount() != 0 {
		t.Errorf("expected 0 active timers, got %d", mt.TimerCount())
	}
}

func TestManualTime_NotFiredBeforeDeadline(t *testing.T) {
	mt := NewManualTime()
	fired := false

	mt.Schedule("x", 5*time.Second, func() { fired = true })

	// Advance only 3s — timer should NOT fire
	mt.Advance(3 * time.Second)
	if fired {
		t.Error("timer should not have fired at 3s")
	}

	// Advance to 5s — timer SHOULD fire
	mt.Advance(2 * time.Second)
	if !fired {
		t.Error("timer should have fired at 5s")
	}
}

func TestManualTime_Cancel(t *testing.T) {
	mt := NewManualTime()
	fired := false

	cancel := mt.Schedule("x", 5*time.Second, func() { fired = true })
	cancel()

	mt.Advance(10 * time.Second)
	if fired {
		t.Error("cancelled timer should not fire")
	}
	if mt.TimerCount() != 0 {
		t.Errorf("expected 0 active timers after cancel, got %d", mt.TimerCount())
	}
}

func TestManualTime_MultipleFiresInOrder(t *testing.T) {
	mt := NewManualTime()
	var order []int

	mt.Schedule("1", 1*time.Second, func() { order = append(order, 1) })
	mt.Schedule("2", 2*time.Second, func() { order = append(order, 2) })
	mt.Schedule("3", 3*time.Second, func() { order = append(order, 3) })

	mt.Advance(5 * time.Second)

	if len(order) != 3 {
		t.Fatalf("expected 3 fires, got %d: %v", len(order), order)
	}
	for i, v := range order {
		if v != i+1 {
			t.Errorf("order[%d] = %d, want %d", i, v, i+1)
		}
	}
}

/*
 * router_resizer_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "testing"

// TestDefaultResizer_GrowUnderPressure verifies that when routees have pending
// messages above the pressure threshold, Capacity returns a positive delta.
func TestDefaultResizer_GrowUnderPressure(t *testing.T) {
	r := &DefaultResizer{
		LowerBound:        1,
		UpperBound:        10,
		PressureThreshold: 1,
		RampupRate:        0.5,
		BackoffRate:       0.3,
		BackoffThreshold:  0.3,
		MessagesPerResize: 1,
	}
	// 2 routees each with 5 pending — both above threshold of 1.
	pending := []int{5, 5}
	delta := r.Capacity(pending)
	if delta <= 0 {
		t.Fatalf("expected positive delta under pressure, got %d", delta)
	}
	// ceil(2 * 0.5) = 1
	if delta != 1 {
		t.Fatalf("expected delta=1 (ceil(2*0.5)), got %d", delta)
	}
}

// TestDefaultResizer_RespectsUpperBound verifies that when the pool is already
// at the upper bound, Capacity returns 0 even under pressure.
func TestDefaultResizer_RespectsUpperBound(t *testing.T) {
	r := &DefaultResizer{
		LowerBound:        1,
		UpperBound:        2,
		PressureThreshold: 1,
		RampupRate:        0.5,
		BackoffRate:       0.3,
		BackoffThreshold:  0.3,
		MessagesPerResize: 1,
	}
	// 2 routees (== UpperBound) with high pending.
	pending := []int{10, 10}
	delta := r.Capacity(pending)
	if delta != 0 {
		t.Fatalf("expected delta=0 at upper bound, got %d", delta)
	}
}

// TestDefaultResizer_RespectsLowerBound verifies that when the pool is at the
// lower bound and routees are idle, Capacity returns 0 (no further shrink).
func TestDefaultResizer_RespectsLowerBound(t *testing.T) {
	r := &DefaultResizer{
		LowerBound:        2,
		UpperBound:        10,
		PressureThreshold: 1,
		RampupRate:        0.5,
		BackoffRate:       0.3,
		BackoffThreshold:  0.3,
		MessagesPerResize: 1,
	}
	// 2 routees (== LowerBound), all idle.
	pending := []int{0, 0}
	delta := r.Capacity(pending)
	if delta != 0 {
		t.Fatalf("expected delta=0 at lower bound, got %d", delta)
	}
}

// TestDefaultResizer_ShrinkOnIdle verifies that when the pool is above the
// lower bound and all routees are idle, Capacity returns a negative delta.
func TestDefaultResizer_ShrinkOnIdle(t *testing.T) {
	r := &DefaultResizer{
		LowerBound:        1,
		UpperBound:        10,
		PressureThreshold: 1,
		RampupRate:        0.5,
		BackoffRate:       0.3,
		BackoffThreshold:  0.3,
		MessagesPerResize: 1,
	}
	// 4 routees, all idle (100% idle > 30% threshold).
	pending := []int{0, 0, 0, 0}
	delta := r.Capacity(pending)
	if delta >= 0 {
		t.Fatalf("expected negative delta on idle, got %d", delta)
	}
	// floor(4 * 0.3) = 1
	if delta != -1 {
		t.Fatalf("expected delta=-1 (floor(4*0.3)), got %d", delta)
	}
}

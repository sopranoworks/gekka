/*
 * phi_accrual_detector_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"
	"time"
)

// simulateHeartbeats sends n heartbeats with the given interval between them.
// It bypasses real-time sleeping by directly manipulating lastHeartbeatAt so
// that the recorded inter-arrival times match the desired interval without
// making the test suite slow.
func simulateHeartbeats(d *PhiAccrualFailureDetector, n int, interval time.Duration) {
	d.mu.Lock()
	// Seed the first beat in the past so the first real Heartbeat() call records
	// a clean interval.
	d.lastHeartbeatAt = time.Now().Add(-time.Duration(n) * interval)
	d.hasFirstBeat = true
	d.mu.Unlock()

	for i := 0; i < n; i++ {
		d.mu.Lock()
		// Move last timestamp forward by exactly one interval, then append.
		d.lastHeartbeatAt = d.lastHeartbeatAt.Add(interval)
		iv := interval
		if len(d.history) >= d.maxSampleSize {
			copy(d.history, d.history[1:])
			d.history = d.history[:len(d.history)-1]
		}
		d.history = append(d.history, iv)
		d.mu.Unlock()
	}
}

// TestPhiStaysLowForRegularHeartbeats verifies that a node sending heartbeats
// on a steady cadence has a very low φ immediately after the last beat.
func TestPhiStaysLowForRegularHeartbeats(t *testing.T) {
	d := New(10.0, 1000, 50*time.Millisecond)

	// Simulate 100 heartbeats at exactly 1s intervals.
	simulateHeartbeats(d, 100, 1*time.Second)

	// Just received the last heartbeat — set lastHeartbeatAt to now.
	d.mu.Lock()
	d.lastHeartbeatAt = time.Now()
	d.mu.Unlock()

	phi := d.Phi()
	if phi > 1.0 {
		t.Errorf("expected low φ immediately after heartbeat, got %.4f", phi)
	}
	if !d.IsAvailable() {
		t.Errorf("node should be available immediately after heartbeat, φ=%.4f", phi)
	}
}

// TestPhiIncreasesAfterSuddenStop verifies that φ climbs significantly once a
// node stops sending heartbeats for multiple times the expected interval.
func TestPhiIncreasesAfterSuddenStop(t *testing.T) {
	d := New(10.0, 1000, 50*time.Millisecond)

	// Train on 100 heartbeats at 100ms intervals.
	simulateHeartbeats(d, 100, 100*time.Millisecond)

	// Set last beat to 2 seconds ago — far beyond the normal interval.
	d.mu.Lock()
	d.lastHeartbeatAt = time.Now().Add(-2 * time.Second)
	d.mu.Unlock()

	phi := d.Phi()
	// mean ≈ 100ms, stdDev ≈ 50ms (minStdDev). t = 2000ms.
	// z = (2000-100)/(50*sqrt2) ≈ 26.9 → pLater ≈ 0 → phi >> 1.
	if phi < 3.0 {
		t.Errorf("expected φ > 3 after long silence, got %.4f", phi)
	}
}

// TestPhiExceedsThresholdWhenNodeAppearsDead verifies that IsAvailable returns
// false when the node has been silent for much longer than its normal interval.
func TestPhiExceedsThresholdWhenNodeAppearsDead(t *testing.T) {
	threshold := 5.0
	d := New(threshold, 1000, 50*time.Millisecond)

	// Train on 200ms intervals.
	simulateHeartbeats(d, 100, 200*time.Millisecond)

	// Set last beat to 10 seconds ago — the node is clearly dead.
	d.mu.Lock()
	d.lastHeartbeatAt = time.Now().Add(-10 * time.Second)
	d.mu.Unlock()

	if d.IsAvailable() {
		t.Errorf("node should be unavailable after 10s silence (φ=%.4f, threshold=%.1f)",
			d.Phi(), threshold)
	}
}

// TestPhiRemainsReasonableUnderJitter verifies that moderate jitter does not
// cause false positives — φ stays below the threshold for a node that is
// responding, albeit with variable latency.
func TestPhiRemainsReasonableUnderJitter(t *testing.T) {
	d := New(10.0, 1000, 50*time.Millisecond)

	// Simulate jittery heartbeats: alternating 80ms and 120ms intervals.
	// Mean ≈ 100ms, stdDev = 20ms.
	half := 500
	intervals := make([]time.Duration, 0, half*2)
	for i := 0; i < half; i++ {
		intervals = append(intervals, 80*time.Millisecond, 120*time.Millisecond)
	}

	d.mu.Lock()
	d.hasFirstBeat = true
	for _, iv := range intervals {
		if len(d.history) >= d.maxSampleSize {
			copy(d.history, d.history[1:])
			d.history = d.history[:len(d.history)-1]
		}
		d.history = append(d.history, iv)
	}
	// Last heartbeat was "just now".
	d.lastHeartbeatAt = time.Now()
	d.mu.Unlock()

	phi := d.Phi()
	if phi > 2.0 {
		t.Errorf("expected low φ under moderate jitter, got %.4f", phi)
	}
	if !d.IsAvailable() {
		t.Errorf("jittery-but-alive node should still be available, φ=%.4f", phi)
	}
}

// TestInitialStateReturnsZeroPhi verifies that a freshly created detector
// returns 0 before any heartbeat is observed.
func TestInitialStateReturnsZeroPhi(t *testing.T) {
	d := New(10.0, 1000, 200*time.Millisecond)
	if phi := d.Phi(); phi != 0.0 {
		t.Errorf("expected 0.0 φ before first heartbeat, got %.4f", phi)
	}
	if d.IsAvailable() {
		t.Error("node with no heartbeats should not be available")
	}
}

// TestSingleHeartbeatSeedsBaseline verifies that exactly one heartbeat is
// enough to start computing meaningful φ values. Before the firstHeartbeat-
// Estimate fix, only one heartbeat left history empty and Phi returned 0
// indefinitely, leaving the detector wedged when a node was muted/killed
// shortly after joining (matching Pekko's actual failure-detector behavior).
//
// After the fix, the first Heartbeat() call seeds history with the
// firstHeartbeatEstimate (default 1s) so Phi computes against a real
// distribution from the first call onward.
func TestSingleHeartbeatSeedsBaseline(t *testing.T) {
	d := New(10.0, 1000, 200*time.Millisecond)
	d.Heartbeat()

	// Immediately after the first heartbeat, timeSinceLast ≈ 0 so the
	// late-probability is high and φ should be close to 0 (well below
	// the threshold). The detector must report the node as available.
	phi := d.Phi()
	if phi >= 10.0 {
		t.Errorf("φ immediately after first heartbeat must be well below threshold, got %.4f", phi)
	}
	if !d.IsAvailable() {
		t.Error("node with one heartbeat must be reported as available")
	}

	// After 5× the firstHeartbeatEstimate (5s by default) of silence,
	// φ must exceed the threshold so the detector can mark the node
	// unreachable. This is the regression test for the wedged-detector
	// bug where φ stayed at 0 forever.
	d.lastHeartbeatAt = time.Now().Add(-5 * time.Second)
	phi = d.Phi()
	if phi < 10.0 {
		t.Errorf("φ after 5s of silence with 1 heartbeat must exceed threshold, got %.4f", phi)
	}
	if d.IsAvailable() {
		t.Error("node silent for 5s after one heartbeat must NOT be available")
	}
}

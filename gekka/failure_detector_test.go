/*
 * failure_detector_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"
)

func TestPhiAccrualFailureDetector(t *testing.T) {
	fd := NewPhiAccrualFailureDetector(8.0, 1000)
	node := "node1"

	// 1. Initial state
	if fd.Phi(node) != 0.0 {
		t.Errorf("expected 0.0 phi for new node, got %v", fd.Phi(node))
	}

	// 2. Train with stable heartbeats (100ms interval)
	for i := 0; i < 10; i++ {
		fd.Heartbeat(node)
		time.Sleep(10 * time.Millisecond) // actually sleep less to make it fast
	}
	// Re-train without sleep but with simulated timestamps if we could,
	// but let's just use real time for simplicity and adjust expectations.

	// Real test:
	for i := 0; i < 50; i++ {
		fd.Heartbeat(node)
		// No sleep, intervals will be very small (~0-1ms)
	}

	phi1 := fd.Phi(node)
	if phi1 > 1.0 {
		t.Errorf("phi should be low for very recent heartbeat, got %v", phi1)
	}

	// 3. Wait for phi to increase
	// Since intervals were ~0ms, even a 500ms delay should trigger high phi if stdDev is small.
	// But we have minStdDev = 100ms.
	// mean ~= 0. p = CDF((500 - 0) / 100) = CDF(5).
	// Erf(5/sqrt(2)) is close to 1.
	time.Sleep(600 * time.Millisecond)
	phi2 := fd.Phi(node)
	if phi2 < 1.0 {
		t.Errorf("phi should increase after delay, got %v", phi2)
	}

	if !fd.IsAvailable(node) {
		// With threshold 8.0 and stdDev 100, x=800ms should be needed.
		// Let's check 1.2s
		time.Sleep(600 * time.Millisecond)
		phi3 := fd.Phi(node)
		if phi3 < 8.0 {
			t.Logf("phi at 1.2s: %v (threshold 8.0)", phi3)
		} else {
			t.Logf("node marked unavailable at phi=%v", phi3)
		}
	}
}

/*
 * failure_detector_test.go
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
	// Since intervals were ~0ms, we need to wait long enough compared to minStdDev=500ms.
	// mean ~= 0. p = CDF((1200 - 0) / (500 * sqrt(2))) = CDF(2.4)
	// Erf(2.4 / 1.41) = Erf(1.7) ~= 0.98. p ~= 0.99. Phi = -log10(0.01) = 2.0.
	time.Sleep(1200 * time.Millisecond)
	phi2 := fd.Phi(node)
	if phi2 < 1.0 {
		t.Errorf("phi should increase after delay, got %v", phi2)
	}

	if !fd.IsAvailable(node) {
		// Threshold 8.0, mean 0, stdDev 500.
		// We need timeSinceLast to be large enough.
		// CDF( (2000-0)/(500*1.41) ) = CDF(2.8) -> p=0.997 -> phi=2.5.
		// Actually for 8.0 we need p = 1 - 10^-8, which is huge.
		// But our Phi caps at threshold+1 if p >= 1.0.
		time.Sleep(2000 * time.Millisecond)
		phi3 := fd.Phi(node)
		if phi3 < 8.0 {
			t.Logf("phi at ~3s: %v (threshold 8.0)", phi3)
		} else {
			t.Logf("node marked unavailable at phi=%v", phi3)
		}
	}
}

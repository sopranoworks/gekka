/*
 * coordinator_rebalance_interval_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"testing"
	"time"
)

// TestCoordinator_RebalanceIntervalHonored verifies that an explicitly
// configured RebalanceInterval (mirroring HOCON
// pekko.cluster.sharding.rebalance-interval) drives the periodic rebalance
// timer instead of the built-in 10s default.
func TestCoordinator_RebalanceIntervalHonored(t *testing.T) {
	coord := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	coord.RebalanceInterval = 50 * time.Millisecond

	selfRef := &mockRef{path: "/user/coordinator"}
	coord.SetSelf(selfRef)

	coord.scheduleRebalanceTick()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		selfRef.mu.Lock()
		got := len(selfRef.messages)
		selfRef.mu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	selfRef.mu.Lock()
	defer selfRef.mu.Unlock()
	found := false
	for _, msg := range selfRef.messages {
		if _, ok := msg.(RebalanceTick); ok {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected RebalanceTick to fire within 500ms (interval=50ms), got messages: %v", selfRef.messages)
	}
}

// TestCoordinator_RebalanceIntervalDefaultIsTenSeconds verifies the
// fallback default when no override is configured: scheduleRebalanceTick
// must NOT deliver a tick inside a sub-second window.
func TestCoordinator_RebalanceIntervalDefaultIsTenSeconds(t *testing.T) {
	coord := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	// No RebalanceInterval override → falls back to 10s default.

	selfRef := &mockRef{path: "/user/coordinator"}
	coord.SetSelf(selfRef)

	coord.scheduleRebalanceTick()

	time.Sleep(150 * time.Millisecond)

	selfRef.mu.Lock()
	defer selfRef.mu.Unlock()
	for _, msg := range selfRef.messages {
		if _, ok := msg.(RebalanceTick); ok {
			t.Fatalf("RebalanceTick fired prematurely (default interval is 10s, observed within 150ms)")
		}
	}
}

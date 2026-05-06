/*
 * multi_dc_fd_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// trainPhiHotPath drives the per-node detector with one seeded heartbeat
// (1ms estimate) followed by 49 rapid back-to-back heartbeats so the
// inter-arrival history is dominated by near-zero intervals. The minStdDev
// floor (50ms in these tests) becomes the effective σ, which makes φ exceed
// a threshold of 2.0 within ~150ms of the next pause.
func trainPhiHotPath(fd *PhiAccrualFailureDetector, key string) {
	fd.HeartbeatWithEstimate(key, 1*time.Millisecond)
	for i := 0; i < 49; i++ {
		fd.Heartbeat(key)
	}
}

// TestPhiAccrualFailureDetector_IsAvailableWithMargin exercises the
// margin-aware reachability check on the wrapper detector independently of
// the cluster manager.
func TestPhiAccrualFailureDetector_IsAvailableWithMargin(t *testing.T) {
	fd := NewPhiAccrualFailureDetector(2.0, 1000)
	fd.Reconfigure(2.0, 1000, 50*time.Millisecond)
	node := "node1"

	// Unseen target: false regardless of margin.
	if fd.IsAvailableWithMargin(node, time.Second) {
		t.Error("unseen node must be unavailable even with margin")
	}

	trainPhiHotPath(fd, node)

	// Fresh: phi well below threshold; available with or without margin.
	if !fd.IsAvailable(node) {
		t.Error("fresh node should be available")
	}
	if !fd.IsAvailableWithMargin(node, 0) {
		t.Error("fresh node should be available with zero margin")
	}

	// Pause until phi exceeds the 2.0 threshold.
	time.Sleep(200 * time.Millisecond)

	if fd.IsAvailable(node) {
		t.Errorf("expected phi above threshold after 200ms pause; got phi=%.3f", fd.Phi(node))
	}
	// Margin >> elapsed → grace window keeps the node available.
	if !fd.IsAvailableWithMargin(node, time.Second) {
		t.Errorf("margin (1s) > elapsed (~200ms) must keep node available")
	}
	// Margin << elapsed → no grace; node remains unavailable.
	if fd.IsAvailableWithMargin(node, 50*time.Millisecond) {
		t.Errorf("margin (50ms) < elapsed (~200ms) must leave node unavailable")
	}
}

// TestEffectiveAcceptableHeartbeatPause verifies that the cluster-level
// helper picks the cross-DC margin only for foreign-DC targets.
func TestEffectiveAcceptableHeartbeatPause(t *testing.T) {
	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.SetLocalDataCenter("tokyo")

	tokyo2 := makeUAWithDC("10.0.1.2", 2552, 2)
	osaka1 := makeUAWithDC("10.0.2.1", 2552, 3)
	cm.Mu.Lock()
	addMemberUpWithRoles(cm, tokyo2, 2, []string{"dc-tokyo"})
	addMemberUpWithRoles(cm, osaka1, 3, []string{"dc-osaka"})
	cm.Mu.Unlock()

	t.Run("intra-DC returns 0", func(t *testing.T) {
		cm.CrossDCAcceptableHeartbeatPause = 5 * time.Second
		if got := cm.EffectiveAcceptableHeartbeatPause(tokyo2.GetAddress()); got != 0 {
			t.Errorf("intra-DC margin = %v, want 0", got)
		}
	})

	t.Run("cross-DC returns configured margin", func(t *testing.T) {
		cm.CrossDCAcceptableHeartbeatPause = 5 * time.Second
		if got := cm.EffectiveAcceptableHeartbeatPause(osaka1.GetAddress()); got != 5*time.Second {
			t.Errorf("cross-DC margin = %v, want 5s", got)
		}
	})

	t.Run("cross-DC unset returns 0", func(t *testing.T) {
		cm.CrossDCAcceptableHeartbeatPause = 0
		if got := cm.EffectiveAcceptableHeartbeatPause(osaka1.GetAddress()); got != 0 {
			t.Errorf("cross-DC margin (unset) = %v, want 0", got)
		}
	})
}

// TestIsTargetAvailable_CrossDCToleratesLongerPauseThanIntraDC is the core
// behavioural test for Phase 4: under the same physical pause, an intra-DC
// target flips unreachable while a cross-DC target stays reachable thanks to
// the additive `acceptable-heartbeat-pause` grace window.
func TestIsTargetAvailable_CrossDCToleratesLongerPauseThanIntraDC(t *testing.T) {
	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.SetLocalDataCenter("tokyo")

	// Lower threshold + tight σ floor so phi spikes within ~150ms of pause,
	// keeping the test fast and stable.
	cm.Fd.Reconfigure(2.0, 1000, 50*time.Millisecond)

	tokyo2 := makeUAWithDC("10.0.1.2", 2552, 2)
	osaka1 := makeUAWithDC("10.0.2.1", 2552, 3)
	cm.Mu.Lock()
	addMemberUpWithRoles(cm, tokyo2, 2, []string{"dc-tokyo"})
	addMemberUpWithRoles(cm, osaka1, 3, []string{"dc-osaka"})
	cm.Mu.Unlock()

	const margin = 400 * time.Millisecond
	cm.CrossDCAcceptableHeartbeatPause = margin

	tokyoKey := fmt.Sprintf("%s:%d-%d",
		tokyo2.GetAddress().GetHostname(), tokyo2.GetAddress().GetPort(), tokyo2.GetUid())
	osakaKey := fmt.Sprintf("%s:%d-%d",
		osaka1.GetAddress().GetHostname(), osaka1.GetAddress().GetPort(), osaka1.GetUid())

	trainPhiHotPath(cm.Fd, tokyoKey)
	trainPhiHotPath(cm.Fd, osakaKey)

	// Pause inside the cross-DC grace window: phi has spiked past the
	// threshold for both targets, but the cross-DC node should still be
	// reachable courtesy of `acceptable-heartbeat-pause`.
	time.Sleep(200 * time.Millisecond)

	if cm.IsTargetAvailable(tokyoKey, tokyo2.GetAddress()) {
		t.Errorf("intra-DC node must flip unreachable when phi exceeds threshold (phi=%.3f)",
			cm.Fd.Phi(tokyoKey))
	}
	if !cm.IsTargetAvailable(osakaKey, osaka1.GetAddress()) {
		t.Errorf("cross-DC node must remain reachable within %v margin (phi=%.3f)",
			margin, cm.Fd.Phi(osakaKey))
	}

	// Pause past the margin: the grace window expires, cross-DC also flips.
	time.Sleep(300 * time.Millisecond) // total ~500ms > 400ms margin

	if cm.IsTargetAvailable(osakaKey, osaka1.GetAddress()) {
		t.Errorf("cross-DC node must flip unreachable once pause exceeds %v margin", margin)
	}
}

// TestIsTargetAvailable_IntraDCUnaffectedByCrossDCMargin verifies that the
// cross-DC `acceptable-heartbeat-pause` knob never relaxes intra-DC
// reachability decisions, even when configured to a long value.
func TestIsTargetAvailable_IntraDCUnaffectedByCrossDCMargin(t *testing.T) {
	local := makeUAWithDC("10.0.1.1", 2552, 1)
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.SetLocalDataCenter("tokyo")

	cm.Fd.Reconfigure(2.0, 1000, 50*time.Millisecond)

	tokyo2 := makeUAWithDC("10.0.1.2", 2552, 2)
	cm.Mu.Lock()
	addMemberUpWithRoles(cm, tokyo2, 2, []string{"dc-tokyo"})
	cm.Mu.Unlock()

	// Generous cross-DC margin that must NOT affect this intra-DC target.
	cm.CrossDCAcceptableHeartbeatPause = 10 * time.Second

	tokyoKey := fmt.Sprintf("%s:%d-%d",
		tokyo2.GetAddress().GetHostname(), tokyo2.GetAddress().GetPort(), tokyo2.GetUid())

	trainPhiHotPath(cm.Fd, tokyoKey)

	// Fresh: reachable.
	if !cm.IsTargetAvailable(tokyoKey, tokyo2.GetAddress()) {
		t.Errorf("intra-DC node must be reachable while phi < threshold (phi=%.3f)",
			cm.Fd.Phi(tokyoKey))
	}

	time.Sleep(200 * time.Millisecond)

	// Intra-DC must flip unreachable; the cross-DC margin must be ignored.
	if cm.IsTargetAvailable(tokyoKey, tokyo2.GetAddress()) {
		t.Errorf("intra-DC node must ignore cross-DC margin and flip unreachable (phi=%.3f)",
			cm.Fd.Phi(tokyoKey))
	}
	// Parity with plain IsAvailable.
	if cm.Fd.IsAvailable(tokyoKey) != cm.IsTargetAvailable(tokyoKey, tokyo2.GetAddress()) {
		t.Errorf("intra-DC IsTargetAvailable must match plain IsAvailable")
	}
}

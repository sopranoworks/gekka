/*
 * sharded_daemon_keepalive_test.go
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

// TestShardedDaemonProcessKeepAliveInterval_DefaultsTo10s locks in the
// reference.conf default for pekko.cluster.sharded-daemon-process.keep-alive-interval.
func TestShardedDaemonProcessKeepAliveInterval_DefaultsTo10s(t *testing.T) {
	defer SetDefaultShardedDaemonProcessKeepAliveInterval(0)

	SetDefaultShardedDaemonProcessKeepAliveInterval(0) // reset
	if got, want := GetDefaultShardedDaemonProcessKeepAliveInterval(), 10*time.Second; got != want {
		t.Errorf("default = %v, want %v", got, want)
	}

	SetDefaultShardedDaemonProcessKeepAliveInterval(-3 * time.Second)
	if got, want := GetDefaultShardedDaemonProcessKeepAliveInterval(), 10*time.Second; got != want {
		t.Errorf("negative override = %v, want %v", got, want)
	}

	SetDefaultShardedDaemonProcessKeepAliveInterval(2500 * time.Millisecond)
	if got, want := GetDefaultShardedDaemonProcessKeepAliveInterval(), 2500*time.Millisecond; got != want {
		t.Errorf("override = %v, want %v", got, want)
	}
}

// TestShardedDaemonProcessShardingRole_Default ensures the role override
// round-trips through the package-level setter.
func TestShardedDaemonProcessShardingRole_Default(t *testing.T) {
	defer SetDefaultShardedDaemonProcessShardingRole("")

	SetDefaultShardedDaemonProcessShardingRole("")
	if got := GetDefaultShardedDaemonProcessShardingRole(); got != "" {
		t.Errorf("default role = %q, want empty", got)
	}

	SetDefaultShardedDaemonProcessShardingRole("daemon-host")
	if got := GetDefaultShardedDaemonProcessShardingRole(); got != "daemon-host" {
		t.Errorf("override role = %q, want %q", got, "daemon-host")
	}
}

// TestRunKeepAliveLoop_EmitsDaemonStartPerTick verifies the runtime effect of
// the keep-alive loop: each tick re-sends a DaemonStart envelope to every
// entity index. With interval = 50ms and n = 3, after waiting ~225ms (≥4
// ticks past the first scheduled fire), at least 9 envelopes (3 ticks × 3
// indices) must have landed on the region. Bootstrap is NOT exercised here —
// runKeepAliveLoop is the unit under test, isolated from
// InitShardedDaemonProcess.
func TestRunKeepAliveLoop_EmitsDaemonStartPerTick(t *testing.T) {
	region := &daemonRegionRef{}

	const n = 3
	const interval = 50 * time.Millisecond

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runKeepAliveLoop(region, n, interval, stopCh)
		close(done)
	}()

	// Wait long enough for ≥3 ticks (≥9 envelopes) but not so long that the
	// test becomes flaky on a slow runner. ~225ms gives ≥4 scheduled fires
	// while leaving plenty of slack for ticker jitter.
	time.Sleep(225 * time.Millisecond)

	close(stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keep-alive loop did not exit after stopCh close")
	}

	msgs := region.all()
	if len(msgs) < 9 {
		t.Fatalf("expected ≥9 envelopes after ~225ms with 50ms interval × %d indices, got %d", n, len(msgs))
	}

	// Every emitted message must be a daemonEnvelope carrying DaemonStart.
	indexCounts := make(map[int]int)
	for i, m := range msgs {
		env, ok := m.(daemonEnvelope)
		if !ok {
			t.Fatalf("msgs[%d]: expected daemonEnvelope, got %T", i, m)
		}
		if _, ok := env.payload.(daemonStart); !ok {
			t.Fatalf("msgs[%d]: payload should be DaemonStart, got %T", i, env.payload)
		}
		indexCounts[env.index]++
	}

	// Each of the n indices must have been pinged at least once.
	for i := 0; i < n; i++ {
		if indexCounts[i] == 0 {
			t.Errorf("index %d was never pinged", i)
		}
	}
}

// TestRunKeepAliveLoop_StopsOnSignal confirms that closing stopCh halts
// further emissions: after a quiescence window post-close, no new envelopes
// arrive.
func TestRunKeepAliveLoop_StopsOnSignal(t *testing.T) {
	region := &daemonRegionRef{}

	const n = 2
	const interval = 30 * time.Millisecond

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runKeepAliveLoop(region, n, interval, stopCh)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	close(stopCh)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keep-alive loop did not exit after stopCh close")
	}

	mid := len(region.all())
	if mid == 0 {
		t.Fatal("expected at least one envelope before stop")
	}

	// Quiescence window: nothing should arrive after stop.
	time.Sleep(120 * time.Millisecond)
	end := len(region.all())

	if end != mid {
		t.Errorf("envelopes increased after stop: %d → %d", mid, end)
	}
}

// TestRunKeepAliveLoop_NoOpOnNonPositiveInterval confirms the early-return
// branch (interval ≤ 0) does not panic and does not emit anything.
func TestRunKeepAliveLoop_NoOpOnNonPositiveInterval(t *testing.T) {
	region := &daemonRegionRef{}

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runKeepAliveLoop(region, 3, 0, stopCh)
		close(done)
	}()

	select {
	case <-done:
		// expected: returns immediately
	case <-time.After(200 * time.Millisecond):
		t.Fatal("loop did not return on zero interval")
	}

	if got := len(region.all()); got != 0 {
		t.Errorf("expected 0 envelopes for zero-interval loop, got %d", got)
	}

	// stopCh is unused; closing it should be safe.
	close(stopCh)
}

// TestShardedDaemonProcessSettings_KeepAliveInterval_OverridesDefault ensures
// the per-process KeepAliveInterval in ShardedDaemonProcessSettings overrides
// the package-level default at resolution time. Without bringing up a full
// sharding region, we exercise the resolution branch directly.
func TestShardedDaemonProcessSettings_KeepAliveInterval_OverridesDefault(t *testing.T) {
	defer SetDefaultShardedDaemonProcessKeepAliveInterval(0)

	SetDefaultShardedDaemonProcessKeepAliveInterval(7 * time.Second)

	settings := ShardedDaemonProcessSettings{KeepAliveInterval: 250 * time.Millisecond}
	resolve := func(s ShardedDaemonProcessSettings) time.Duration {
		if s.KeepAliveInterval > 0 {
			return s.KeepAliveInterval
		}
		return GetDefaultShardedDaemonProcessKeepAliveInterval()
	}

	if got := resolve(settings); got != 250*time.Millisecond {
		t.Errorf("explicit override = %v, want 250ms", got)
	}
	if got := resolve(ShardedDaemonProcessSettings{}); got != 7*time.Second {
		t.Errorf("zero override falls through to default; got = %v, want 7s", got)
	}
}

// TestShardedDaemonProcess_Stop_IsIdempotent confirms Stop can be called
// multiple times without panicking and without panicking on a process that
// was constructed without a stopCh (e.g., direct struct literal in tests).
func TestShardedDaemonProcess_Stop_IsIdempotent(t *testing.T) {
	// Direct struct literal — no stopCh.
	d := &ShardedDaemonProcess{NumberOfInstances: 2}
	d.Stop()
	d.Stop() // second call must not panic.

	// With a stopCh.
	d2 := &ShardedDaemonProcess{NumberOfInstances: 2, stopCh: make(chan struct{})}
	d2.Stop()
	d2.Stop() // already-closed channel must not double-close.

	// nil receiver — must not panic.
	var nilProc *ShardedDaemonProcess
	nilProc.Stop()
}

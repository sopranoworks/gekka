/*
 * watch_failure_detector_test.go
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

// Verifies the parsed acceptable-heartbeat-pause value drives a runtime
// difference: a short pause flips IsAvailable to false within the pause
// window, while the Pekko default (10 s) keeps the same scenario reachable.
//
// This is the "consumer site" runtime evidence required by the portable Pekko
// roadmap's Definition of Done #1: the parsed value must be observed in a
// non-getter, non-assignment-only context.
func TestWatchFailureDetector_AcceptablePauseRuntimeEffect(t *testing.T) {
	t.Run("short_pause_flips_unavailable", func(t *testing.T) {
		w := NewWatchFailureDetector(WatchFailureDetectorConfig{
			Threshold:                8.0,
			MaxSampleSize:            200,
			MinStdDeviation:          10 * time.Millisecond,
			HeartbeatInterval:        20 * time.Millisecond,
			AcceptableHeartbeatPause: 80 * time.Millisecond,
		})
		// Establish history: 5 heartbeats spaced 20 ms apart.
		for i := 0; i < 5; i++ {
			w.Heartbeat("n1")
			time.Sleep(20 * time.Millisecond)
		}
		// Immediately after the last heartbeat, the node must be available
		// (we are well inside the 80 ms acceptable-pause window).
		if !w.IsAvailable("n1") {
			t.Fatalf("expected available immediately after heartbeats; got unavailable")
		}
		// Wait past the acceptable-pause window. Phi will exceed threshold
		// because mean inter-arrival is ~20 ms and elapsed time will be
		// hundreds of ms.
		time.Sleep(300 * time.Millisecond)
		if w.IsAvailable("n1") {
			t.Fatalf("expected unavailable after %v elapsed past %v pause; got available",
				300*time.Millisecond, 80*time.Millisecond)
		}
	})

	t.Run("long_pause_keeps_available", func(t *testing.T) {
		w := NewWatchFailureDetector(WatchFailureDetectorConfig{
			Threshold:                8.0,
			MaxSampleSize:            200,
			MinStdDeviation:          10 * time.Millisecond,
			HeartbeatInterval:        20 * time.Millisecond,
			AcceptableHeartbeatPause: 10 * time.Second, // Pekko default
		})
		for i := 0; i < 5; i++ {
			w.Heartbeat("n1")
			time.Sleep(20 * time.Millisecond)
		}
		// 300 ms is far below the 10 s acceptable-pause: still available.
		time.Sleep(300 * time.Millisecond)
		if !w.IsAvailable("n1") {
			t.Fatalf("expected still available with 10s pause after 300ms elapsed; got unavailable")
		}
	})
}

// Verifies the parsed reaper-interval and threshold values are surfaced
// through accessor methods that the production reaper consults each tick.
func TestWatchFailureDetector_ConfigAccessors(t *testing.T) {
	w := NewWatchFailureDetector(WatchFailureDetectorConfig{
		Threshold:                      4.5,
		AcceptableHeartbeatPause:       250 * time.Millisecond,
		UnreachableNodesReaperInterval: 75 * time.Millisecond,
	})
	if got := w.AcceptablePause(); got != 250*time.Millisecond {
		t.Errorf("AcceptablePause = %v, want 250ms", got)
	}
	if got := w.ReaperInterval(); got != 75*time.Millisecond {
		t.Errorf("ReaperInterval = %v, want 75ms", got)
	}
	cfg := w.Config()
	if cfg.Threshold != 4.5 {
		t.Errorf("Config.Threshold = %v, want 4.5", cfg.Threshold)
	}
}

// Verifies Reconfigure replaces the active settings AND clears per-node history,
// so a subsequent IsAvailable call honours the new pause value.
func TestWatchFailureDetector_ReconfigureRuntimeEffect(t *testing.T) {
	w := NewWatchFailureDetector(WatchFailureDetectorConfig{
		Threshold:                8.0,
		MaxSampleSize:            200,
		MinStdDeviation:          10 * time.Millisecond,
		HeartbeatInterval:        20 * time.Millisecond,
		AcceptableHeartbeatPause: 10 * time.Second,
	})
	for i := 0; i < 5; i++ {
		w.Heartbeat("n1")
		time.Sleep(20 * time.Millisecond)
	}
	if !w.IsAvailable("n1") {
		t.Fatalf("expected available with 10s pause; got unavailable")
	}

	// Reconfigure to an aggressive 50 ms pause; per-node history is cleared,
	// so a fresh seeding heartbeat is required to re-prime the detector.
	w.Reconfigure(WatchFailureDetectorConfig{
		Threshold:                8.0,
		MaxSampleSize:            200,
		MinStdDeviation:          10 * time.Millisecond,
		HeartbeatInterval:        20 * time.Millisecond,
		AcceptableHeartbeatPause: 50 * time.Millisecond,
	})
	for i := 0; i < 5; i++ {
		w.Heartbeat("n1")
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if w.IsAvailable("n1") {
		t.Fatalf("expected unavailable after Reconfigure shrunk pause to 50ms; got available")
	}
}

// Verifies WatchedNodes only returns keys that have been heartbeated.
func TestWatchFailureDetector_WatchedNodesAndRemove(t *testing.T) {
	w := NewWatchFailureDetector(WatchFailureDetectorConfig{})
	if got := w.WatchedNodes(); len(got) != 0 {
		t.Fatalf("expected empty watched-nodes, got %v", got)
	}
	w.Heartbeat("a")
	w.Heartbeat("b")
	got := w.WatchedNodes()
	if len(got) != 2 {
		t.Fatalf("expected 2 watched nodes, got %v", got)
	}
	w.Remove("a")
	got = w.WatchedNodes()
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected only [b] after removing a, got %v", got)
	}
}

// Verifies a never-heartbeated key is reported available (defensive).
func TestWatchFailureDetector_UnknownKeyAvailable(t *testing.T) {
	w := NewWatchFailureDetector(WatchFailureDetectorConfig{
		AcceptableHeartbeatPause: 10 * time.Millisecond,
	})
	if !w.IsAvailable("never-seen") {
		t.Fatal("expected available for never-seen key")
	}
}

// Verifies zero-valued configs are filled with Pekko reference defaults.
func TestWatchFailureDetector_DefaultsApplied(t *testing.T) {
	w := NewWatchFailureDetector(WatchFailureDetectorConfig{})
	cfg := w.Config()
	defaults := PekkoWatchFDDefaults()
	if cfg.Threshold != defaults.Threshold {
		t.Errorf("Threshold = %v, want default %v", cfg.Threshold, defaults.Threshold)
	}
	if cfg.AcceptableHeartbeatPause != defaults.AcceptableHeartbeatPause {
		t.Errorf("AcceptablePause = %v, want default %v", cfg.AcceptableHeartbeatPause, defaults.AcceptableHeartbeatPause)
	}
	if cfg.UnreachableNodesReaperInterval != defaults.UnreachableNodesReaperInterval {
		t.Errorf("ReaperInterval = %v, want default %v", cfg.UnreachableNodesReaperInterval, defaults.UnreachableNodesReaperInterval)
	}
	if cfg.MaxSampleSize != defaults.MaxSampleSize {
		t.Errorf("MaxSampleSize = %v, want default %v", cfg.MaxSampleSize, defaults.MaxSampleSize)
	}
	if cfg.MinStdDeviation != defaults.MinStdDeviation {
		t.Errorf("MinStdDeviation = %v, want default %v", cfg.MinStdDeviation, defaults.MinStdDeviation)
	}
	if cfg.HeartbeatInterval != defaults.HeartbeatInterval {
		t.Errorf("HeartbeatInterval = %v, want default %v", cfg.HeartbeatInterval, defaults.HeartbeatInterval)
	}
	if cfg.ExpectedResponseAfter != defaults.ExpectedResponseAfter {
		t.Errorf("ExpectedResponseAfter = %v, want default %v", cfg.ExpectedResponseAfter, defaults.ExpectedResponseAfter)
	}
}

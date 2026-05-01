/*
 * watch_failure_detector.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"sync"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// WatchFailureDetectorConfig tunes the remote-watch failure detector.
// Maps to the Pekko HOCON namespace pekko.remote.watch-failure-detector.*.
//
// All durations are zero-initialised friendly: a zero value falls back to the
// Pekko reference defaults inside NewWatchFailureDetector / Reconfigure.
type WatchFailureDetectorConfig struct {
	// ImplementationClass is the FQCN of the failure-detector implementation.
	// Pekko default: "org.apache.pekko.remote.PhiAccrualFailureDetector".
	// gekka only ships PhiAccrualFailureDetector; any other value parses
	// for HOCON faithfulness but is not loaded as a runtime class.
	ImplementationClass string

	// HeartbeatInterval is the cadence at which keep-alive heartbeats are
	// sent to a remote-watch peer. Pekko default: 1s.
	// (gekka feeds the detector via the existing cluster heartbeat path; the
	// value is recorded for parity and surfaces in tests but does not yet
	// drive a separate watch-heartbeat scheduler.)
	HeartbeatInterval time.Duration

	// Threshold is the φ value above which a remote-watch peer is declared
	// unreachable. Pekko default: 10.0.
	Threshold float64

	// MaxSampleSize is the sliding-window size for inter-heartbeat history.
	// Pekko default: 200.
	MaxSampleSize int

	// MinStdDeviation is the lower bound on σ inside the φ calculation.
	// Pekko default: 100ms.
	MinStdDeviation time.Duration

	// AcceptableHeartbeatPause is the duration of missed heartbeats that
	// will not flip the detector to unavailable. Pekko default: 10s.
	// Enforced at the WatchFailureDetector wrapper level (see IsAvailable).
	AcceptableHeartbeatPause time.Duration

	// UnreachableNodesReaperInterval is the cadence at which the watch-FD
	// reaper polls IsAvailable for every watched node and triggers
	// remote-death notifications. Pekko default: 1s.
	UnreachableNodesReaperInterval time.Duration

	// ExpectedResponseAfter is the expected interval between a heartbeat
	// request and its response. Pekko default: 1s. Currently informational
	// in gekka — recorded for parity.
	ExpectedResponseAfter time.Duration
}

// PekkoWatchFDDefaults returns a WatchFailureDetectorConfig populated with
// Pekko's reference.conf default values for pekko.remote.watch-failure-detector.*.
func PekkoWatchFDDefaults() WatchFailureDetectorConfig {
	return WatchFailureDetectorConfig{
		ImplementationClass:            "org.apache.pekko.remote.PhiAccrualFailureDetector",
		HeartbeatInterval:              1 * time.Second,
		Threshold:                      10.0,
		MaxSampleSize:                  200,
		MinStdDeviation:                100 * time.Millisecond,
		AcceptableHeartbeatPause:       10 * time.Second,
		UnreachableNodesReaperInterval: 1 * time.Second,
		ExpectedResponseAfter:          1 * time.Second,
	}
}

// withDefaults returns cfg with any zero-valued field replaced by the Pekko
// default. ImplementationClass is left as-is (empty string is a valid sentinel
// for "use the bundled detector").
func (cfg WatchFailureDetectorConfig) withDefaults() WatchFailureDetectorConfig {
	d := PekkoWatchFDDefaults()
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = d.HeartbeatInterval
	}
	if cfg.Threshold <= 0 {
		cfg.Threshold = d.Threshold
	}
	if cfg.MaxSampleSize <= 0 {
		cfg.MaxSampleSize = d.MaxSampleSize
	}
	if cfg.MinStdDeviation <= 0 {
		cfg.MinStdDeviation = d.MinStdDeviation
	}
	if cfg.AcceptableHeartbeatPause <= 0 {
		cfg.AcceptableHeartbeatPause = d.AcceptableHeartbeatPause
	}
	if cfg.UnreachableNodesReaperInterval <= 0 {
		cfg.UnreachableNodesReaperInterval = d.UnreachableNodesReaperInterval
	}
	if cfg.ExpectedResponseAfter <= 0 {
		cfg.ExpectedResponseAfter = d.ExpectedResponseAfter
	}
	return cfg
}

// WatchFailureDetector tracks heartbeat history for the subset of remote nodes
// being death-watched and reports availability based on Phi-Accrual analysis
// plus an acceptable-heartbeat-pause grace window. It is distinct from the
// cluster membership failure detector (cluster.PhiAccrualFailureDetector); the
// two have independent thresholds, sample windows, and pause budgets.
type WatchFailureDetector struct {
	mu        sync.RWMutex
	cfg       WatchFailureDetectorConfig
	detectors map[string]*icluster.PhiAccrualFailureDetector
	lastBeat  map[string]time.Time
}

// NewWatchFailureDetector constructs a WatchFailureDetector with the given
// configuration; zero-valued fields are filled with Pekko reference defaults.
func NewWatchFailureDetector(cfg WatchFailureDetectorConfig) *WatchFailureDetector {
	return &WatchFailureDetector{
		cfg:       cfg.withDefaults(),
		detectors: make(map[string]*icluster.PhiAccrualFailureDetector),
		lastBeat:  make(map[string]time.Time),
	}
}

// Config returns the active configuration (with defaults applied).
func (w *WatchFailureDetector) Config() WatchFailureDetectorConfig {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cfg
}

// ReaperInterval returns the configured unreachable-nodes-reaper-interval.
func (w *WatchFailureDetector) ReaperInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cfg.UnreachableNodesReaperInterval
}

// AcceptablePause returns the configured acceptable-heartbeat-pause.
func (w *WatchFailureDetector) AcceptablePause() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cfg.AcceptableHeartbeatPause
}

// Heartbeat records the arrival of a heartbeat for the given node key. The
// per-node Phi-Accrual detector is allocated lazily on first call. The
// configured heartbeat-interval is used as the first-heartbeat synthetic
// estimate so the very first elapsed-time check has a realistic baseline
// (without this seed the underlying detector would inflate the mean by a
// hard-coded 1 s, distorting Phi for sub-second heartbeat cadences).
func (w *WatchFailureDetector) Heartbeat(key string) {
	w.mu.Lock()
	d, ok := w.detectors[key]
	if !ok {
		d = icluster.NewWithFirstEstimate(
			w.cfg.Threshold,
			w.cfg.MaxSampleSize,
			w.cfg.MinStdDeviation,
			w.cfg.HeartbeatInterval,
		)
		w.detectors[key] = d
	}
	w.lastBeat[key] = time.Now()
	w.mu.Unlock()
	d.Heartbeat()
}

// IsAvailable reports whether the watched node is still considered reachable.
// Returns true when the node has never been seen (defensive: only the reaper
// iterates registered keys, and never-seen keys cannot be in that set anyway).
// Returns true while inside the acceptable-heartbeat-pause window since the
// most recent heartbeat. Otherwise consults the per-node Phi-Accrual detector.
func (w *WatchFailureDetector) IsAvailable(key string) bool {
	w.mu.RLock()
	d, ok := w.detectors[key]
	last, hasBeat := w.lastBeat[key]
	pause := w.cfg.AcceptableHeartbeatPause
	w.mu.RUnlock()

	if !ok || !hasBeat {
		return true
	}
	if time.Since(last) <= pause {
		return true
	}
	return d.IsAvailable()
}

// Remove drops all per-node state for key. Called after delivering a Terminated
// notification (so the reaper does not re-fire) and from unwatch when no
// watchers remain.
func (w *WatchFailureDetector) Remove(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.detectors, key)
	delete(w.lastBeat, key)
}

// WatchedNodes returns a snapshot of every key currently tracked. Used by the
// reaper to iterate the active set without holding the lock during callbacks.
func (w *WatchFailureDetector) WatchedNodes() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	keys := make([]string, 0, len(w.detectors))
	for k := range w.detectors {
		keys = append(keys, k)
	}
	return keys
}

// Reconfigure replaces the active configuration and discards all per-node
// history (matching PhiAccrualFailureDetector.Reconfigure semantics). Safe to
// call before the cluster has been joined; in practice no live death-watch
// detection is lost since heartbeats resume on the next interval.
func (w *WatchFailureDetector) Reconfigure(cfg WatchFailureDetectorConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg = cfg.withDefaults()
	w.detectors = make(map[string]*icluster.PhiAccrualFailureDetector)
	w.lastBeat = make(map[string]time.Time)
}

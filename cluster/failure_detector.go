/*
 * failure_detector.go
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

// PhiAccrualFailureDetector manages per-node PhiAccrualFailureDetector instances.
// Each remote node gets its own detector so history is never mixed between nodes.
type PhiAccrualFailureDetector struct {
	mu        sync.RWMutex
	detectors map[string]*icluster.PhiAccrualFailureDetector
	threshold float64
	// configuration forwarded to each per-node detector
	maxSampleSize   int
	minStdDeviation time.Duration
}

// FailureDetectorConfig holds tunable parameters for the PhiAccrualFailureDetector.
// All fields are optional; zero values fall back to safe defaults.
type FailureDetectorConfig struct {
	// Threshold is the φ value above which a node is declared unreachable.
	// Corresponds to HOCON: gekka.cluster.failure-detector.threshold
	// Default: 10.0
	Threshold float64

	// MaxSampleSize is the sliding-window size for heartbeat inter-arrival history.
	// Corresponds to HOCON: gekka.cluster.failure-detector.max-sample-size
	// Default: 1000
	MaxSampleSize int

	// MinStdDeviation is the lower bound on σ to prevent φ explosions when
	// heartbeats are very regular or the window is small.
	// Corresponds to HOCON: gekka.cluster.failure-detector.min-std-deviation
	// Default: 500ms
	MinStdDeviation time.Duration
}

func NewPhiAccrualFailureDetector(threshold float64, windowSize int) *PhiAccrualFailureDetector {
	return &PhiAccrualFailureDetector{
		detectors:       make(map[string]*icluster.PhiAccrualFailureDetector),
		threshold:       threshold,
		maxSampleSize:   windowSize,
		minStdDeviation: 500 * time.Millisecond,
	}
}

// Reconfigure updates the detector parameters and clears all per-node history so
// subsequent heartbeats use the new settings.  Safe to call before the cluster
// has been joined (no active detection history is lost in practice).
func (fd *PhiAccrualFailureDetector) Reconfigure(threshold float64, maxSamples int, minStdDev time.Duration) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.threshold = threshold
	fd.maxSampleSize = maxSamples
	fd.minStdDeviation = minStdDev
	fd.detectors = make(map[string]*icluster.PhiAccrualFailureDetector)
}

func (fd *PhiAccrualFailureDetector) detectorFor(nodeKey string) *icluster.PhiAccrualFailureDetector {
	fd.mu.RLock()
	d, ok := fd.detectors[nodeKey]
	fd.mu.RUnlock()
	if ok {
		return d
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	// Double-checked locking.
	if d, ok = fd.detectors[nodeKey]; ok {
		return d
	}
	d = icluster.New(fd.threshold, fd.maxSampleSize, fd.minStdDeviation)
	fd.detectors[nodeKey] = d
	return d
}

func (fd *PhiAccrualFailureDetector) Heartbeat(nodeKey string) {
	fd.detectorFor(nodeKey).Heartbeat()
}

func (fd *PhiAccrualFailureDetector) Phi(nodeKey string) float64 {
	fd.mu.RLock()
	d, ok := fd.detectors[nodeKey]
	fd.mu.RUnlock()
	if !ok {
		return 0.0
	}
	return d.Phi()
}

func (fd *PhiAccrualFailureDetector) IsAvailable(nodeKey string) bool {
	fd.mu.RLock()
	d, ok := fd.detectors[nodeKey]
	fd.mu.RUnlock()
	if !ok {
		return false
	}
	return d.IsAvailable()
}

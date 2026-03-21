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

func NewPhiAccrualFailureDetector(threshold float64, windowSize int) *PhiAccrualFailureDetector {
	return &PhiAccrualFailureDetector{
		detectors:       make(map[string]*icluster.PhiAccrualFailureDetector),
		threshold:       threshold,
		maxSampleSize:   windowSize,
		minStdDeviation: 500 * time.Millisecond,
	}
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

/*
 * failure_detector.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"math"
	"sync"
	"time"
)

// PhiAccrualFailureDetector implements the failure detector as described in Hayashibara et al.
type PhiAccrualFailureDetector struct {
	mu            sync.RWMutex
	lastHeartbeat map[string]time.Time
	intervals     map[string][]float64
	windowSize    int
	threshold     float64
	minStdDev     float64
}

func NewPhiAccrualFailureDetector(threshold float64, windowSize int) *PhiAccrualFailureDetector {
	return &PhiAccrualFailureDetector{
		lastHeartbeat: make(map[string]time.Time),
		intervals:     make(map[string][]float64),
		windowSize:    windowSize,
		threshold:     threshold,
		minStdDev:     100.0, // ms, standard min deviation to prevent tiny values
	}
}

func (fd *PhiAccrualFailureDetector) Heartbeat(nodeKey string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()
	if last, ok := fd.lastHeartbeat[nodeKey]; ok {
		interval := float64(now.Sub(last).Milliseconds())
		fd.intervals[nodeKey] = append(fd.intervals[nodeKey], interval)
		if len(fd.intervals[nodeKey]) > fd.windowSize {
			fd.intervals[nodeKey] = fd.intervals[nodeKey][1:]
		}
	}
	fd.lastHeartbeat[nodeKey] = now
}

func (fd *PhiAccrualFailureDetector) Phi(nodeKey string) float64 {
	fd.mu.RLock()
	last, ok := fd.lastHeartbeat[nodeKey]
	intervals := fd.intervals[nodeKey]
	fd.mu.RUnlock()

	if !ok || len(intervals) < 1 {
		return 0.0
	}

	timeSinceLast := float64(time.Since(last).Milliseconds())

	mean := 0.0
	for _, v := range intervals {
		mean += v
	}
	mean /= float64(len(intervals))

	variance := 0.0
	for _, v := range intervals {
		variance += math.Pow(v-mean, 2)
	}
	variance /= float64(len(intervals))
	stdDev := math.Sqrt(variance)
	if stdDev < fd.minStdDev {
		stdDev = fd.minStdDev
	}

	// Normal distribution CDF: F(x) = 0.5 * (1 + Erf((x - mean) / (stdDev * sqrt(2))))
	p := 0.5 * (1.0 + math.Erf((timeSinceLast-mean)/(stdDev*math.Sqrt(2))))

	// Phi = -log10(1 - p)
	if p >= 1.0 {
		return fd.threshold + 1.0 // cap it
	}
	phi := -math.Log10(1.0 - p)
	return phi
}

func (fd *PhiAccrualFailureDetector) IsAvailable(nodeKey string) bool {
	fd.mu.RLock()
	_, ok := fd.lastHeartbeat[nodeKey]
	fd.mu.RUnlock()
	if !ok {
		return false
	}
	return fd.Phi(nodeKey) < fd.threshold
}

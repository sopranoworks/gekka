/*
 * phi_accrual_detector.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cluster contains internal cluster infrastructure primitives.
package cluster

import (
	"math"
	"sync"
	"time"
)

const (
	DefaultThreshold     = 10.0
	DefaultMaxSampleSize = 1000
	DefaultMinStdDev     = 200 * time.Millisecond
)

// PhiAccrualFailureDetector tracks the heartbeat history of a single remote
// node and computes a suspicion level (φ) based on the normal distribution of
// observed inter-arrival times (Hayashibara et al.).
//
// φ = -log₁₀( P_later(t) ) where P_later(t) is derived from the CDF of the
// normal distribution fitted to the sliding window of intervals.
//
// A node is considered unavailable when φ exceeds the configured threshold.
type PhiAccrualFailureDetector struct {
	threshold        float64
	maxSampleSize    int
	minStdDeviation  time.Duration
	history          []time.Duration // sliding window of inter-arrival times
	lastHeartbeatAt  time.Time
	hasFirstBeat     bool
	mu               sync.Mutex
}

// New creates a PhiAccrualFailureDetector for a single remote node.
//   - threshold:       φ value above which the node is declared unavailable (e.g. 10.0).
//   - maxSampleSize:   maximum number of intervals retained in the sliding window (e.g. 1000).
//   - minStdDeviation: lower bound on σ to prevent φ from exploding on perfectly
//                      regular heartbeats or tiny windows (e.g. 200ms).
func New(threshold float64, maxSampleSize int, minStdDeviation time.Duration) *PhiAccrualFailureDetector {
	return &PhiAccrualFailureDetector{
		threshold:       threshold,
		maxSampleSize:   maxSampleSize,
		minStdDeviation: minStdDeviation,
		history:         make([]time.Duration, 0, maxSampleSize),
	}
}

// Heartbeat records the arrival of a heartbeat from the remote node.
// The first call seeds the last-arrival timestamp without recording an interval.
func (d *PhiAccrualFailureDetector) Heartbeat() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if d.hasFirstBeat {
		interval := now.Sub(d.lastHeartbeatAt)
		if len(d.history) >= d.maxSampleSize {
			// Slide: drop oldest entry.
			copy(d.history, d.history[1:])
			d.history = d.history[:len(d.history)-1]
		}
		d.history = append(d.history, interval)
	}
	d.lastHeartbeatAt = now
	d.hasFirstBeat = true
}

// Phi returns the current suspicion level for the remote node.
// Returns 0 if fewer than two heartbeats have been observed.
func (d *PhiAccrualFailureDetector) Phi() float64 {
	d.mu.Lock()
	hasFirst := d.hasFirstBeat
	last := d.lastHeartbeatAt
	history := make([]time.Duration, len(d.history))
	copy(history, d.history)
	d.mu.Unlock()

	if !hasFirst || len(history) == 0 {
		return 0.0
	}

	timeSinceLast := time.Since(last)

	// Compute mean and variance of the interval history.
	var sum float64
	for _, iv := range history {
		sum += float64(iv.Milliseconds())
	}
	mean := sum / float64(len(history))

	var varSum float64
	for _, iv := range history {
		diff := float64(iv.Milliseconds()) - mean
		varSum += diff * diff
	}
	stdDev := math.Sqrt(varSum / float64(len(history)))

	minSD := float64(d.minStdDeviation.Milliseconds())
	if stdDev < minSD {
		stdDev = minSD
	}

	// P_later(t) = 1 - CDF(t) = 0.5 * erfc( (t - mean) / (stdDev * sqrt(2)) )
	t := float64(timeSinceLast.Milliseconds())
	pLater := 0.5 * math.Erfc((t-mean)/(stdDev*math.Sqrt2))

	if pLater <= 0 {
		// Guard: return a value clearly beyond threshold so callers know the
		// node is gone without producing +Inf.
		return d.threshold + 1.0
	}

	return -math.Log10(pLater)
}

// IsAvailable returns true when φ < threshold, i.e. the node is considered
// reachable.  Returns false for unseen nodes (no heartbeat ever recorded).
func (d *PhiAccrualFailureDetector) IsAvailable() bool {
	d.mu.Lock()
	has := d.hasFirstBeat
	d.mu.Unlock()
	if !has {
		return false
	}
	return d.Phi() < d.threshold
}

/*
 * router_resizer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "math"

// Resizer defines the interface for automatic pool router scaling.
// Capacity is called periodically with the current pending message count
// per routee, and returns a signed delta: positive to grow, negative to
// shrink, zero for no change.
type Resizer interface {
	// Capacity returns the signed change in pool size based on per-routee
	// pending message counts. Positive = grow, negative = shrink, zero = no change.
	Capacity(pendingPerRoutee []int) int
}

// DefaultResizer implements the Pekko-compatible automatic resizer algorithm.
//
// When routee pressure (fraction of routees with pending messages above
// PressureThreshold) is positive, the pool grows by ceil(currentSize * RampupRate),
// capped at UpperBound. When all routees are idle and the idle fraction exceeds
// BackoffThreshold, the pool shrinks by floor(currentSize * BackoffRate), floored
// at LowerBound.
type DefaultResizer struct {
	// LowerBound is the minimum number of routees (floor for shrink).
	LowerBound int
	// UpperBound is the maximum number of routees (cap for grow).
	UpperBound int
	// PressureThreshold is the mailbox depth above which a routee is considered
	// under pressure.
	PressureThreshold int
	// RampupRate is the fraction of the current pool size to add when growing.
	RampupRate float64
	// BackoffRate is the fraction of the current pool size to remove when shrinking.
	BackoffRate float64
	// BackoffThreshold is the minimum idle fraction required to trigger shrink.
	BackoffThreshold float64
	// MessagesPerResize is the number of messages between resize evaluations.
	MessagesPerResize int
}

// Capacity implements Resizer. It examines the per-routee pending counts and
// returns a signed delta.
func (r *DefaultResizer) Capacity(pendingPerRoutee []int) int {
	currentSize := len(pendingPerRoutee)
	if currentSize == 0 {
		return 0
	}

	// Count routees under pressure (pending > PressureThreshold).
	pressureCount := 0
	for _, p := range pendingPerRoutee {
		if p > r.PressureThreshold {
			pressureCount++
		}
	}

	pressure := float64(pressureCount) / float64(currentSize)

	if pressure > 0 {
		// Already at upper bound — no room to grow.
		if currentSize >= r.UpperBound {
			return 0
		}
		// Grow by ceil(currentSize * RampupRate), minimum 1.
		delta := int(math.Ceil(float64(currentSize) * r.RampupRate))
		if delta < 1 {
			delta = 1
		}
		// Cap so we don't exceed UpperBound.
		if currentSize+delta > r.UpperBound {
			delta = r.UpperBound - currentSize
		}
		return delta
	}

	// pressure == 0: all routees are at or below threshold.
	// Calculate idle fraction (routees with 0 pending).
	idleCount := 0
	for _, p := range pendingPerRoutee {
		if p == 0 {
			idleCount++
		}
	}
	idleFraction := float64(idleCount) / float64(currentSize)

	if idleFraction > r.BackoffThreshold {
		// Already at lower bound — no room to shrink.
		if currentSize <= r.LowerBound {
			return 0
		}
		// Shrink by floor(currentSize * BackoffRate), minimum 1.
		delta := int(math.Floor(float64(currentSize) * r.BackoffRate))
		if delta < 1 {
			delta = 1
		}
		// Floor so we don't go below LowerBound.
		if currentSize-delta < r.LowerBound {
			delta = currentSize - r.LowerBound
		}
		return -delta
	}

	return 0
}

/*
 * backoff_options.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"math"
	"math/rand"
	"time"
)

// BackoffOptions configures exponential backoff behavior for supervisor strategies.
type BackoffOptions struct {
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	RandomFactor  float64 // 0.0 to 1.0 for jitter
	ResetInterval time.Duration
}

// NextDelay calculates the duration to wait before the next attempt.
func (o BackoffOptions) NextDelay(failures int) time.Duration {
	if failures <= 0 {
		return o.MinBackoff
	}

	// backoff = min * 2^failures
	backoff := float64(o.MinBackoff) * math.Pow(2, float64(failures))
	
	// clamp to MaxBackoff
	if backoff > float64(o.MaxBackoff) {
		backoff = float64(o.MaxBackoff)
	}

	// Apply jitter
	if o.RandomFactor > 0 {
		jitter := (rand.Float64()*2 - 1) * o.RandomFactor * backoff
		backoff += jitter
	}

	// Ensure we don't go below MinBackoff due to jitter
	if backoff < float64(o.MinBackoff) {
		backoff = float64(o.MinBackoff)
	}

	return time.Duration(backoff)
}

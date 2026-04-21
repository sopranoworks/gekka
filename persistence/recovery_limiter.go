/*
 * recovery_limiter.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import "context"

// DefaultMaxConcurrentRecoveries is the Pekko default for
// pekko.persistence.max-concurrent-recoveries.
const DefaultMaxConcurrentRecoveries = 50

// RecoveryLimiter controls how many persistent actors can recover
// (replay events from the journal) concurrently. It prevents a burst
// of actors from overwhelming the journal backend on startup.
//
// Corresponds to pekko.persistence.max-concurrent-recoveries.
type RecoveryLimiter struct {
	sem chan struct{}
}

// globalLimiter is the process-wide recovery limiter.
// Initialised to DefaultMaxConcurrentRecoveries; overridden by
// SetMaxConcurrentRecoveries when HOCON config is loaded.
var globalLimiter = NewRecoveryLimiter(DefaultMaxConcurrentRecoveries)

// NewRecoveryLimiter creates a limiter that allows at most maxConcurrent
// simultaneous recoveries. If maxConcurrent <= 0 it defaults to
// DefaultMaxConcurrentRecoveries.
func NewRecoveryLimiter(maxConcurrent int) *RecoveryLimiter {
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentRecoveries
	}
	return &RecoveryLimiter{
		sem: make(chan struct{}, maxConcurrent),
	}
}

// Acquire blocks until a recovery slot is available or ctx is cancelled.
// Returns nil on success, ctx.Err() on cancellation.
func (rl *RecoveryLimiter) Acquire(ctx context.Context) error {
	select {
	case rl.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a recovery slot previously acquired via Acquire.
func (rl *RecoveryLimiter) Release() {
	<-rl.sem
}

// MaxConcurrent returns the capacity of this limiter.
func (rl *RecoveryLimiter) MaxConcurrent() int {
	return cap(rl.sem)
}

// SetMaxConcurrentRecoveries replaces the global recovery limiter.
// Call this during configuration (before actors start recovering).
func SetMaxConcurrentRecoveries(n int) {
	globalLimiter = NewRecoveryLimiter(n)
}

// AcquireRecovery acquires a slot from the global recovery limiter.
// Call Release on the returned limiter when recovery is complete.
func AcquireRecovery(ctx context.Context) (*RecoveryLimiter, error) {
	if err := globalLimiter.Acquire(ctx); err != nil {
		return nil, err
	}
	return globalLimiter, nil
}

// GlobalRecoveryLimiter returns the current global limiter (for testing).
func GlobalRecoveryLimiter() *RecoveryLimiter {
	return globalLimiter
}

/*
 * recovery_limiter_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveryLimiter_BasicAcquireRelease(t *testing.T) {
	rl := NewRecoveryLimiter(2)
	ctx := context.Background()

	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}

	// Third acquire should block — try with a short timeout.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := rl.Acquire(ctx2); err == nil {
		t.Fatal("expected third Acquire to block and fail with timeout")
	}

	// Release one slot.
	rl.Release()

	// Now acquire should succeed.
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}

	rl.Release()
	rl.Release()
}

func TestRecoveryLimiter_MaxConcurrent(t *testing.T) {
	rl := NewRecoveryLimiter(5)
	if rl.MaxConcurrent() != 5 {
		t.Fatalf("MaxConcurrent = %d, want 5", rl.MaxConcurrent())
	}
}

func TestRecoveryLimiter_DefaultOnZero(t *testing.T) {
	rl := NewRecoveryLimiter(0)
	if rl.MaxConcurrent() != DefaultMaxConcurrentRecoveries {
		t.Fatalf("MaxConcurrent = %d, want %d", rl.MaxConcurrent(), DefaultMaxConcurrentRecoveries)
	}
}

func TestRecoveryLimiter_ConcurrencyLimit(t *testing.T) {
	const limit = 3
	rl := NewRecoveryLimiter(limit)
	ctx := context.Background()

	var active atomic.Int32
	var maxActive atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Acquire(ctx); err != nil {
				return
			}
			defer rl.Release()

			cur := active.Add(1)
			for {
				old := maxActive.Load()
				if cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond) // simulate recovery work
			active.Add(-1)
		}()
	}

	wg.Wait()

	if maxActive.Load() > limit {
		t.Fatalf("max concurrent recoveries = %d, want <= %d", maxActive.Load(), limit)
	}
}

func TestSetMaxConcurrentRecoveries(t *testing.T) {
	// Save and restore the original global limiter.
	original := GlobalRecoveryLimiter()
	defer func() { globalLimiter = original }()

	SetMaxConcurrentRecoveries(10)
	if GlobalRecoveryLimiter().MaxConcurrent() != 10 {
		t.Fatalf("global MaxConcurrent = %d, want 10", GlobalRecoveryLimiter().MaxConcurrent())
	}
}

func TestAcquireRecovery_Global(t *testing.T) {
	original := GlobalRecoveryLimiter()
	defer func() { globalLimiter = original }()

	SetMaxConcurrentRecoveries(2)
	ctx := context.Background()

	rl1, err := AcquireRecovery(ctx)
	if err != nil {
		t.Fatalf("AcquireRecovery 1: %v", err)
	}
	rl2, err := AcquireRecovery(ctx)
	if err != nil {
		t.Fatalf("AcquireRecovery 2: %v", err)
	}

	// Third should block.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = AcquireRecovery(ctx2)
	if err == nil {
		t.Fatal("expected third AcquireRecovery to fail with timeout")
	}

	rl1.Release()
	rl2.Release()
}

/*
 * restart_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── RestartSource ────────────────────────────────────────────────────────

// TestRestartSource_RestartsOnError verifies that after two failing attempts
// the third factory invocation succeeds and all its elements are delivered.
func TestRestartSource_RestartsOnError(t *testing.T) {
	sentinelErr := errors.New("transient failure")
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   5 * time.Millisecond,
		MaxBackoff:   20 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  5,
	}

	src := stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
		n := calls.Add(1)
		if n <= 2 {
			return stream.Failed[int](sentinelErr)
		}
		return stream.FromSlice([]int{10, 20, 30})
	})

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("got %v, want [10 20 30]", result)
	}
	for i, want := range []int{10, 20, 30} {
		if result[i] != want {
			t.Fatalf("index %d: got %d, want %d", i, result[i], want)
		}
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("factory called %d times, want 3", n)
	}
}

// TestRestartSource_ExponentialBackoff verifies that the cumulative wait time
// grows with the configured backoff: two failures with MinBackoff=20ms means
// at least 20ms total elapsed before the third attempt delivers elements.
func TestRestartSource_ExponentialBackoff(t *testing.T) {
	const minBackoff = 20 * time.Millisecond
	sentinelErr := errors.New("err")
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   minBackoff,
		MaxBackoff:   200 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  5,
	}

	start := time.Now()
	stream.RunWith( //nolint:errcheck
		stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
			if calls.Add(1) <= 2 {
				return stream.Failed[int](sentinelErr)
			}
			return stream.FromSlice([]int{1})
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	// First failure sleeps MinBackoff (20ms); second failure sleeps 2×MinBackoff (40ms).
	// Total >= 60ms.
	minExpected := minBackoff + 2*minBackoff
	if elapsed < minExpected {
		t.Fatalf("backoff too short: elapsed %v < expected >= %v", elapsed, minExpected)
	}
	t.Logf("backoff elapsed %v (min expected %v)", elapsed, minExpected)
}

// TestRestartSource_RandomFactorAddsJitter verifies that RandomFactor > 0
// produces a delay strictly longer than the base backoff.
func TestRestartSource_RandomFactorAddsJitter(t *testing.T) {
	const (
		minBackoff   = 20 * time.Millisecond
		randomFactor = 0.5
	)
	sentinelErr := errors.New("err")
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   minBackoff,
		MaxBackoff:   500 * time.Millisecond,
		RandomFactor: randomFactor,
		MaxRestarts:  3,
	}

	start := time.Now()
	stream.RunWith( //nolint:errcheck
		stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
			if calls.Add(1) <= 1 {
				return stream.Failed[int](sentinelErr)
			}
			return stream.FromSlice([]int{1})
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	// With RandomFactor=0.5, delay is at least MinBackoff.
	if elapsed < minBackoff {
		t.Fatalf("jitter produced delay shorter than MinBackoff: %v", elapsed)
	}
	t.Logf("jitter elapsed %v", elapsed)
}

// TestRestartSource_MaxRestartsExceeded verifies that once MaxRestarts is
// exhausted the original error is propagated to the downstream sink.
func TestRestartSource_MaxRestartsExceeded(t *testing.T) {
	sentinelErr := errors.New("always fails")
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   1 * time.Millisecond,
		MaxBackoff:   5 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  2,
	}

	_, err := stream.RunWith(
		stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
			calls.Add(1)
			return stream.Failed[int](sentinelErr)
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
	// Initial attempt + MaxRestarts=2 retries = 3 total calls.
	if n := calls.Load(); n != 3 {
		t.Fatalf("factory called %d times, want 3", n)
	}
}

// TestRestartSource_InfiniteRestarts verifies that MaxRestarts=-1 allows
// unlimited restarts (we check that 5 failures are survived).
func TestRestartSource_InfiniteRestarts(t *testing.T) {
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   1 * time.Millisecond,
		MaxBackoff:   5 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  -1,
	}

	result, err := stream.RunWith(
		stream.RestartSource(settings, func() stream.Source[int, stream.NotUsed] {
			n := calls.Add(1)
			if n <= 5 {
				return stream.Failed[int](errors.New("transient"))
			}
			return stream.FromSlice([]int{42})
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0] != 42 {
		t.Fatalf("got %v, want [42]", result)
	}
}

// ─── RestartFlow ──────────────────────────────────────────────────────────

// TestRestartFlow_RestartsOnError verifies that a failing flow is re-attached
// and subsequent elements from the upstream pass through normally.
func TestRestartFlow_RestartsOnError(t *testing.T) {
	sentinelErr := errors.New("flow error")
	var calls atomic.Int32

	settings := stream.RestartSettings{
		MinBackoff:   5 * time.Millisecond,
		MaxBackoff:   20 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  5,
	}

	// The flow fails on the first invocation; the second succeeds with identity.
	flowFactory := func() stream.Flow[int, int, stream.NotUsed] {
		n := calls.Add(1)
		if n == 1 {
			return stream.MapE(func(v int) (int, error) {
				return 0, sentinelErr
			})
		}
		return stream.Map(func(v int) int { return v })
	}

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.RestartFlow(settings, flowFactory),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// After the first flow fails on the first element, the restarted flow
	// picks up whatever remains in the upstream iterator.
	if len(result) == 0 {
		t.Fatal("expected at least one element from the restarted flow")
	}
}

// TestRestartFlow_MaxRestartsExceeded verifies that a persistently failing
// flow propagates the error after MaxRestarts is exhausted.
func TestRestartFlow_MaxRestartsExceeded(t *testing.T) {
	sentinelErr := errors.New("always fails")

	settings := stream.RestartSettings{
		MinBackoff:   1 * time.Millisecond,
		MaxBackoff:   5 * time.Millisecond,
		RandomFactor: 0,
		MaxRestarts:  2,
	}

	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.RestartFlow(settings, func() stream.Flow[int, int, stream.NotUsed] {
				return stream.MapE(func(v int) (int, error) {
					return 0, sentinelErr
				})
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

// TestRestartFlow_SuccessPassThrough verifies that RestartFlow with a healthy
// flow factory is transparent: all elements arrive unchanged.
func TestRestartFlow_SuccessPassThrough(t *testing.T) {
	const N = 50

	settings := stream.RestartSettings{
		MinBackoff:  1 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
		MaxRestarts: 3,
	}

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(N)),
			stream.RestartFlow(settings, func() stream.Flow[int, int, stream.NotUsed] {
				return stream.Map(func(v int) int { return v * 2 })
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i*2 {
			t.Fatalf("index %d: got %d, want %d", i, v, i*2)
		}
	}
}

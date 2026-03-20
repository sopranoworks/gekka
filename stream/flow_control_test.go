/*
 * flow_control_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── Buffer ───────────────────────────────────────────────────────────────

// TestBuffer_Backpressure_DeliversAll verifies that OverflowBackpressure
// preserves every element: the upstream goroutine blocks when the buffer is
// full, so nothing is lost regardless of how slow the consumer is.
func TestBuffer_Backpressure_DeliversAll(t *testing.T) {
	const N = 200

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(N)),
			stream.Buffer[int](8, stream.OverflowBackpressure),
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
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestBuffer_DropHead_OldElementsDiscarded verifies that OverflowDropHead
// evicts the oldest buffered element when the buffer is full, so a slow
// consumer receives the most recent data rather than stale data.
//
// Setup: source [0..4], buffer size 2, consumer sleeps 50 ms between reads.
// The producer outpaces the consumer and overflows the buffer several times.
// By the time the consumer reads its second element the buffer holds [3, 4]
// (elements 1 and 2 were evicted by DropHead).  Expected: [0, 3, 4].
func TestBuffer_DropHead_OldElementsDiscarded(t *testing.T) {
	const (
		bufSize   = 2
		srcSize   = 5
		sleepTime = 50 * time.Millisecond
	)

	var received []int
	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(srcSize)),
			stream.Buffer[int](bufSize, stream.OverflowDropHead),
		),
		stream.ForeachErr(func(n int) error {
			time.Sleep(sleepTime) // slow consumer: let the producer overflow the buffer
			received = append(received, n)
			return nil
		}),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// At least some elements must have been dropped.
	if len(received) >= srcSize {
		t.Fatalf("expected fewer than %d elements (DropHead should drop old ones), got all %d",
			srcSize, len(received))
	}

	// Elements must arrive in ascending order (buffer preserves insertion order).
	for i := 1; i < len(received); i++ {
		if received[i] <= received[i-1] {
			t.Fatalf("elements out of order at index %d: %v", i, received)
		}
	}

	// The LAST element received must be the newest value (srcSize-1),
	// proving that recent elements are retained over stale ones.
	if last := received[len(received)-1]; last != srcSize-1 {
		t.Fatalf("last received element is %d, want %d (newest)", last, srcSize-1)
	}
}

// TestBuffer_DropTail_NewElementsDiscarded verifies that OverflowDropTail
// discards the incoming element when the buffer is full, so a slow consumer
// receives the earliest data while newly-produced elements are dropped.
//
// Setup: source [0..4], buffer size 2, consumer sleeps 50 ms between reads.
// After the consumer reads element 0, the producer fills [1, 2] and drops 3
// and 4.  Expected received: [0, 1, 2].
func TestBuffer_DropTail_NewElementsDiscarded(t *testing.T) {
	const (
		bufSize   = 2
		srcSize   = 5
		sleepTime = 50 * time.Millisecond
	)

	var received []int
	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(srcSize)),
			stream.Buffer[int](bufSize, stream.OverflowDropTail),
		),
		stream.ForeachErr(func(n int) error {
			time.Sleep(sleepTime)
			received = append(received, n)
			return nil
		}),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) >= srcSize {
		t.Fatalf("expected fewer than %d elements (DropTail should drop new ones), got all %d",
			srcSize, len(received))
	}

	// Elements must arrive in ascending order.
	for i := 1; i < len(received); i++ {
		if received[i] <= received[i-1] {
			t.Fatalf("elements out of order at index %d: %v", i, received)
		}
	}

	// The FIRST element received must be 0 (oldest), proving old elements are
	// kept over newer ones with DropTail.
	if received[0] != 0 {
		t.Fatalf("first received element is %d, want 0 (oldest)", received[0])
	}

	// The last received element must be strictly less than srcSize-1, proving
	// that the newest elements were dropped.
	if last := received[len(received)-1]; last >= srcSize-1 {
		t.Fatalf("last received %d ≥ %d: newest elements should have been dropped",
			last, srcSize-1)
	}
}

// TestBuffer_Fail_FailsOnOverflow verifies that OverflowFail terminates the
// stream with ErrBufferFull the first time the buffer overflows.
func TestBuffer_Fail_FailsOnOverflow(t *testing.T) {
	const (
		bufSize   = 2
		srcSize   = 10
		sleepTime = 20 * time.Millisecond
	)

	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(srcSize)),
			stream.Buffer[int](bufSize, stream.OverflowFail),
		),
		stream.ForeachErr(func(n int) error {
			time.Sleep(sleepTime) // slow consumer: provoke overflow
			return nil
		}),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected ErrBufferFull, got nil")
	}
	if !errors.Is(err, stream.ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

// TestBuffer_Method_OnFlow verifies that the method form
// Flow.Buffer(...) composes correctly with an existing flow.
func TestBuffer_Method_OnFlow(t *testing.T) {
	const N = 100

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(N)),
			stream.Map(func(n int) int { return n * 2 }).Buffer(32, stream.OverflowBackpressure),
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

// TestBuffer_Method_OnSource verifies that Source.Buffer(...) composes
// correctly when chained directly off a Source.
func TestBuffer_Method_OnSource(t *testing.T) {
	const N = 50

	result, err := stream.RunWith(
		stream.FromSlice(makeRange(N)).Buffer(16, stream.OverflowBackpressure),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
}

// ─── Throttle ─────────────────────────────────────────────────────────────

// TestThrottle_LimitsRate verifies that throughput is capped to the configured
// rate.  With 5 elements per 100 ms and burst=1, emitting 6 elements requires
// at least (6-1) × 20 ms = 100 ms.
func TestThrottle_LimitsRate(t *testing.T) {
	const (
		count    = 6
		elements = 5
		per      = 100 * time.Millisecond
		burst    = 1
		// interval = per/elements = 20ms; min time = (count-burst)*interval
		minExpected = time.Duration(count-burst) * (per / elements)
	)

	start := time.Now()
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(count)),
			stream.Throttle[int](elements, per, burst),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != count {
		t.Fatalf("got %d elements, want %d", len(result), count)
	}
	if elapsed < minExpected {
		t.Fatalf("too fast: elapsed %v < minimum expected %v", elapsed, minExpected)
	}
	t.Logf("elapsed %v (min expected %v)", elapsed, minExpected)
}

// TestThrottle_BurstAllowsInitialBurst verifies that a burst > 1 allows the
// first `burst` elements to pass through without any rate-limiting delay.
func TestThrottle_BurstAllowsInitialBurst(t *testing.T) {
	const (
		burstSize = 5
		total     = 5 // emit exactly burst elements — no wait needed
		elements  = 1
		per       = 10 * time.Second // extremely slow rate after burst
	)

	start := time.Now()
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(total)),
			stream.Throttle[int](elements, per, burstSize),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != total {
		t.Fatalf("got %d elements, want %d", len(result), total)
	}
	// With burst=5, all 5 elements consume the pre-filled bucket — no sleep.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("burst tokens not used: elapsed %v (expected < 500ms)", elapsed)
	}
	t.Logf("burst delivery in %v", elapsed)
}

// TestThrottle_Method_OnFlow verifies the method form Flow.Throttle(...).
func TestThrottle_Method_OnFlow(t *testing.T) {
	const (
		count    = 4
		elements = 2
		per      = 100 * time.Millisecond
		burst    = 1
		minTime  = time.Duration(count-burst) * (per / elements)
	)

	start := time.Now()
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(count)),
			stream.Map(func(n int) int { return n }).Throttle(elements, per, burst),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != count {
		t.Fatalf("got %d elements, want %d", len(result), count)
	}
	if elapsed < minTime {
		t.Fatalf("too fast: elapsed %v < %v", elapsed, minTime)
	}
}

// ─── Delay ────────────────────────────────────────────────────────────────

// TestDelay_AddsInterElementDelay verifies that Delay inserts the specified
// pause between elements: N elements with delay d must take at least N×d.
func TestDelay_AddsInterElementDelay(t *testing.T) {
	const (
		count     = 5
		delayTime = 20 * time.Millisecond
		minTotal  = time.Duration(count) * delayTime
	)

	start := time.Now()
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(count)),
			stream.Delay[int](delayTime),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != count {
		t.Fatalf("got %d elements, want %d", len(result), count)
	}
	if elapsed < minTotal {
		t.Fatalf("delay not enforced: elapsed %v < minimum %v", elapsed, minTotal)
	}
	t.Logf("elapsed %v (min expected %v)", elapsed, minTotal)
}

// TestDelay_PreservesOrder verifies that elements pass through in the same
// order regardless of the delay applied.
func TestDelay_PreservesOrder(t *testing.T) {
	const N = 10

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(N)),
			stream.Delay[int](2*time.Millisecond),
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
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestDelay_Method_OnSource verifies that Source.Delay(...) composes
// correctly when chained directly off a Source.
func TestDelay_Method_OnSource(t *testing.T) {
	const (
		count     = 3
		delayTime = 10 * time.Millisecond
	)

	start := time.Now()
	result, err := stream.RunWith(
		stream.FromSlice(makeRange(count)).Delay(delayTime),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != count {
		t.Fatalf("got %d elements, want %d", len(result), count)
	}
	if elapsed < time.Duration(count)*delayTime {
		t.Fatalf("delay not applied via Source.Delay: elapsed %v", elapsed)
	}
}

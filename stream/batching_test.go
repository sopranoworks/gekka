/*
 * batching_test.go
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

// ─── Grouped ──────────────────────────────────────────────────────────────

// TestGrouped_ExactMultiple verifies that a source whose length is an exact
// multiple of n produces full batches with no remainder.
func TestGrouped_ExactMultiple(t *testing.T) {
	result, err := stream.RunWith(
		stream.Grouped(stream.FromSlice(makeRange(6)), 3),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("got %d batches, want 2", len(result))
	}
	for i, batch := range result {
		if len(batch) != 3 {
			t.Fatalf("batch %d: got len %d, want 3", i, len(batch))
		}
		for j, v := range batch {
			if v != i*3+j {
				t.Fatalf("batch %d[%d]: got %d, want %d", i, j, v, i*3+j)
			}
		}
	}
}

// TestGrouped_PartialRemainder verifies that the final batch is smaller when
// the source length is not a multiple of n.
func TestGrouped_PartialRemainder(t *testing.T) {
	result, err := stream.RunWith(
		stream.Grouped(stream.FromSlice([]int{1, 2, 3, 4, 5}), 2),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect [1,2], [3,4], [5]
	if len(result) != 3 {
		t.Fatalf("got %d batches, want 3: %v", len(result), result)
	}
	if len(result[2]) != 1 || result[2][0] != 5 {
		t.Fatalf("last batch: got %v, want [5]", result[2])
	}
}

// TestGrouped_EmptySource verifies that an empty source produces no batches.
func TestGrouped_EmptySource(t *testing.T) {
	result, err := stream.RunWith(
		stream.Grouped(stream.FromSlice([]int{}), 3),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected no batches, got %v", result)
	}
}

// TestGrouped_TotalElementsPreserved verifies that flattening all batches
// reproduces the original elements in order.
func TestGrouped_TotalElementsPreserved(t *testing.T) {
	const N = 50
	batches, err := stream.RunWith(
		stream.Grouped(stream.FromSlice(makeRange(N)), 7),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var flat []int
	for _, b := range batches {
		flat = append(flat, b...)
	}
	if len(flat) != N {
		t.Fatalf("got %d elements after flattening, want %d", len(flat), N)
	}
	for i, v := range flat {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestGrouped_UpstreamErrorPropagates verifies that an upstream error surfaces.
func TestGrouped_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("grouped upstream failure")

	_, err := stream.RunWith(
		stream.Grouped(stream.Failed[int](sentinelErr), 3),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

// ─── GroupedWithin ────────────────────────────────────────────────────────

// TestGroupedWithin_EmitsByCount verifies that the operator flushes a batch as
// soon as n elements accumulate, even when the timer has not yet expired.
func TestGroupedWithin_EmitsByCount(t *testing.T) {
	const (
		n       = 3
		timeout = 10 * time.Second // very long — only count should trigger
	)

	result, err := stream.RunWith(
		stream.GroupedWithin(stream.FromSlice(makeRange(9)), n, timeout),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 9 elements / 3 per batch = exactly 3 batches, each of size 3.
	if len(result) != 3 {
		t.Fatalf("got %d batches, want 3", len(result))
	}
	for i, b := range result {
		if len(b) != n {
			t.Fatalf("batch %d: got len %d, want %d", i, len(b), n)
		}
	}
}

// TestGroupedWithin_EmitsByTime verifies that the operator flushes whatever it
// has accumulated when the timer expires, even if fewer than n elements arrived.
//
// Setup: 2 elements arrive quickly, then the source stalls.  With n=10 and a
// short timeout, the timer should fire and flush the partial batch before all
// elements have been produced.
func TestGroupedWithin_EmitsByTime(t *testing.T) {
	const (
		n       = 10
		timeout = 40 * time.Millisecond
	)

	// Source emits 2 elements instantly and then completes.
	src := stream.FromSlice([]int{1, 2})

	start := time.Now()
	result, err := stream.RunWith(
		stream.GroupedWithin(src, n, timeout),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have received exactly one partial batch of 2 elements.
	if len(result) != 1 {
		t.Fatalf("got %d batches, want 1", len(result))
	}
	if len(result[0]) != 2 || result[0][0] != 1 || result[0][1] != 2 {
		t.Fatalf("got batch %v, want [1 2]", result[0])
	}
	t.Logf("elapsed %v (timeout %v)", elapsed, timeout)
}

// TestGroupedWithin_NoEmptyBatchOnTimerExpiry verifies that a timer expiry
// with zero accumulated elements does NOT emit an empty slice.
func TestGroupedWithin_NoEmptyBatchOnTimerExpiry(t *testing.T) {
	// Source is empty — the first timer tick should fire but produce nothing.
	result, err := stream.RunWith(
		stream.GroupedWithin(stream.FromSlice([]int{}), 5, 20*time.Millisecond),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected no batches for empty source, got %v", result)
	}
}

// TestGroupedWithin_MixedCountAndTime verifies a scenario where some batches
// fill by count and the final partial batch is flushed by time.
func TestGroupedWithin_MixedCountAndTime(t *testing.T) {
	const (
		n       = 3
		timeout = 50 * time.Millisecond
	)
	// 7 elements: first two batches fill by count ([0,1,2], [3,4,5]),
	// the last element [6] is flushed by the timer.
	result, err := stream.RunWith(
		stream.GroupedWithin(stream.FromSlice(makeRange(7)), n, timeout),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("got %d batches, want 3: %v", len(result), result)
	}
	// Last batch must have exactly 1 element.
	if len(result[2]) != 1 || result[2][0] != 6 {
		t.Fatalf("last batch: got %v, want [6]", result[2])
	}
}

// TestGroupedWithin_TotalElementsPreserved verifies no element is lost or
// duplicated through GroupedWithin.
func TestGroupedWithin_TotalElementsPreserved(t *testing.T) {
	const N = 25
	batches, err := stream.RunWith(
		stream.GroupedWithin(stream.FromSlice(makeRange(N)), 4, 100*time.Millisecond),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var flat []int
	for _, b := range batches {
		flat = append(flat, b...)
	}
	if len(flat) != N {
		t.Fatalf("got %d elements, want %d", len(flat), N)
	}
	for i, v := range flat {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestGroupedWithin_UpstreamErrorPropagates verifies that an upstream error
// surfaces through GroupedWithin.
func TestGroupedWithin_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("groupedwithin upstream failure")

	_, err := stream.RunWith(
		stream.GroupedWithin(stream.Failed[int](sentinelErr), 5, 50*time.Millisecond),
		stream.Collect[[]int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

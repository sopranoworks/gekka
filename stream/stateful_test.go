/*
 * stateful_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// ─── StatefulMapConcat ────────────────────────────────────────────────────

// TestStatefulMapConcat_RunningSum verifies that state threads correctly
// across elements: each emission is the cumulative sum up to that point.
func TestStatefulMapConcat_RunningSum(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5})

	result, err := stream.RunWith(
		stream.StatefulMapConcat(src,
			func() int { return 0 },
			func(sum, n int) (int, []int) { return sum + n, []int{sum + n} },
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 3, 6, 10, 15}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestStatefulMapConcat_Duplicate verifies that returning multiple elements per
// input works correctly: each n emits [n, n*2].
func TestStatefulMapConcat_Duplicate(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})

	result, err := stream.RunWith(
		stream.StatefulMapConcat(src,
			func() struct{} { return struct{}{} },
			func(s struct{}, n int) (struct{}, []int) {
				return s, []int{n, n * 2}
			},
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 2, 4, 3, 6}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestStatefulMapConcat_EmptyOutputActsAsFilter verifies that returning an
// empty slice skips the element, acting as a stateful filter.
func TestStatefulMapConcat_EmptyOutputActsAsFilter(t *testing.T) {
	// Only emit even numbers (state unused).
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	result, err := stream.RunWith(
		stream.StatefulMapConcat(src,
			func() struct{} { return struct{}{} },
			func(s struct{}, n int) (struct{}, []int) {
				if n%2 == 0 {
					return s, []int{n}
				}
				return s, nil
			},
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{2, 4, 6}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestStatefulMapConcat_StateIsolatedPerMaterialization verifies that each
// RunWith call gets a fresh state from create(), not shared state.
func TestStatefulMapConcat_StateIsolatedPerMaterialization(t *testing.T) {
	src := stream.FromSlice([]int{10, 20, 30})

	factory := stream.StatefulMapConcat(src,
		func() int { return 0 },
		func(sum, n int) (int, []int) { return sum + n, []int{sum + n} },
	)

	// Run twice — each should start from sum=0.
	for run := range 2 {
		result, err := stream.RunWith(factory, stream.Collect[int](), stream.ActorMaterializer{})
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", run, err)
		}
		want := []int{10, 30, 60}
		if len(result) != len(want) {
			t.Fatalf("run %d: got %v, want %v", run, result, want)
		}
		for i, v := range result {
			if v != want[i] {
				t.Fatalf("run %d index %d: got %d, want %d", run, i, v, want[i])
			}
		}
	}
}

// TestStatefulMapConcat_UpstreamErrorPropagates verifies that an upstream error
// surfaces correctly.
func TestStatefulMapConcat_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("stateful upstream failure")

	_, err := stream.RunWith(
		stream.StatefulMapConcat(stream.Failed[int](sentinelErr),
			func() int { return 0 },
			func(s, n int) (int, []int) { return s + n, []int{s + n} },
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

// TestStatefulMapConcat_TypeTransform verifies that the output type may differ
// from the input type (state + int → string).
func TestStatefulMapConcat_TypeTransform(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})

	result, err := stream.RunWith(
		stream.StatefulMapConcat(src,
			func() int { return 0 },
			func(count, n int) (int, []string) {
				return count + 1, []string{fmt.Sprintf("%d:%d", count+1, n)}
			},
		),
		stream.Collect[string](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"1:1", "2:2", "3:3"}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %q, want %q", i, v, want[i])
		}
	}
}

// ─── FilterMap ────────────────────────────────────────────────────────────

// TestFilterMap_KeepEvenHalved verifies the combined filter+transform: only
// even numbers are kept and each is halved.
func TestFilterMap_KeepEvenHalved(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	result, err := stream.RunWith(
		stream.FilterMap(src, func(n int) (int, bool) {
			if n%2 == 0 {
				return n / 2, true
			}
			return 0, false
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestFilterMap_TypeChange verifies int→string transform with selective keep.
func TestFilterMap_TypeChange(t *testing.T) {
	src := stream.FromSlice([]int{-1, 0, 3, -2, 5})

	result, err := stream.RunWith(
		stream.FilterMap(src, func(n int) (string, bool) {
			if n > 0 {
				return fmt.Sprintf("pos:%d", n), true
			}
			return "", false
		}),
		stream.Collect[string](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"pos:3", "pos:5"}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %q, want %q", i, v, want[i])
		}
	}
}

// TestFilterMap_AllDropped verifies that dropping every element yields an
// empty result without error.
func TestFilterMap_AllDropped(t *testing.T) {
	result, err := stream.RunWith(
		stream.FilterMap(stream.FromSlice([]int{1, 2, 3}), func(int) (int, bool) {
			return 0, false
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

// TestFilterMap_AllKept verifies that keeping every element is equivalent to
// a plain Map.
func TestFilterMap_AllKept(t *testing.T) {
	const N = 10
	result, err := stream.RunWith(
		stream.FilterMap(stream.FromSlice(makeRange(N)), func(n int) (int, bool) {
			return n * 3, true
		}),
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
		if v != i*3 {
			t.Fatalf("index %d: got %d, want %d", i, v, i*3)
		}
	}
}

// TestFilterMap_UpstreamErrorPropagates verifies that upstream errors surface
// through FilterMap.
func TestFilterMap_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("filtermap upstream failure")

	_, err := stream.RunWith(
		stream.FilterMap(stream.Failed[int](sentinelErr), func(n int) (int, bool) {
			return n, true
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
}

// TestFilterMap_WithVia verifies FilterMap composes correctly after Via.
func TestFilterMap_WithVia(t *testing.T) {
	// Double first, then keep only multiples of 4.
	src := stream.Via(
		stream.FromSlice([]int{1, 2, 3, 4, 5}),
		stream.Map(func(n int) int { return n * 2 }),
	)
	result, err := stream.RunWith(
		stream.FilterMap(src, func(n int) (int, bool) {
			return n, n%4 == 0
		}),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{4, 8} // 2*2=4, 4*2=8
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

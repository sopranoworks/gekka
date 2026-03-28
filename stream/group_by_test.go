/*
 * group_by_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// TestGroupBySubFlow_EvenOddSums verifies that GroupBySubFlow correctly routes
// integers to "even" and "odd" sub-streams and that each sub-stream produces
// the expected sum when re-merged.
func TestGroupBySubFlow_EvenOddSums(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	merged := stream.GroupBySubFlow(src, 2, func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	}).MergeSubstreams()

	result, err := stream.RunWith(merged, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(result))
	}

	evenSum := 0
	oddSum := 0
	for _, v := range result {
		if v%2 == 0 {
			evenSum += v
		} else {
			oddSum += v
		}
	}

	// even: 2+4+6 = 12
	if evenSum != 12 {
		t.Errorf("even sum: got %d, want 12", evenSum)
	}
	// odd: 1+3+5 = 9
	if oddSum != 9 {
		t.Errorf("odd sum: got %d, want 9", oddSum)
	}
}

// TestGroupBySubFlow_DistinctPaths verifies that each sub-stream contains only
// the elements belonging to its key.
func TestGroupBySubFlow_DistinctPaths(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	substreams, err := stream.RunWith(
		stream.GroupBy(src, 2, func(n int) string {
			if n%2 == 0 {
				return "even"
			}
			return "odd"
		}),
		stream.Collect[stream.SubStream[int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(substreams) != 2 {
		t.Fatalf("expected 2 sub-streams, got %d", len(substreams))
	}

	for _, ss := range substreams {
		elems, err := stream.RunWith(ss.Source, stream.Collect[int](), stream.ActorMaterializer{})
		if err != nil {
			t.Fatalf("sub-stream %q: unexpected error: %v", ss.Key, err)
		}
		for _, v := range elems {
			var want string
			if v%2 == 0 {
				want = "even"
			} else {
				want = "odd"
			}
			if ss.Key != want {
				t.Errorf("element %d in sub-stream %q but expected %q", v, ss.Key, want)
			}
		}
	}
}

// TestGroupBySubFlow_Filter verifies that SubFlow.Filter correctly narrows
// elements within each sub-stream before merging.
func TestGroupBySubFlow_Filter(t *testing.T) {
	// Only keep values > 2 within each sub-stream.
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	merged := stream.GroupBySubFlow(src, 2, func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	}).Filter(func(n int) bool {
		return n > 2
	}).MergeSubstreams()

	result, err := stream.RunWith(merged, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Expected: even > 2 → {4,6}, odd > 2 → {3,5}
	sort.Ints(result)
	want := []int{3, 4, 5, 6}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("got %v, want %v", result, want)
		}
	}
}

// TestGroupBySubFlow_Backpressure_MaxSubstreams verifies that GroupBySubFlow
// enforces the maxSubstreams limit and pauses (fails) the source when the limit
// is exceeded rather than silently accepting more keys.
func TestGroupBySubFlow_Backpressure_MaxSubstreams(t *testing.T) {
	// maxSubstreams=1 but the key function produces 2 distinct keys ("even"/"odd").
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	merged := stream.GroupBySubFlow(src, 1, func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	}).MergeSubstreams()

	_, err := stream.RunWith(merged, stream.Collect[int](), stream.ActorMaterializer{})
	if !errors.Is(err, stream.ErrTooManySubstreams) {
		t.Fatalf("expected ErrTooManySubstreams, got %v", err)
	}
}

// TestGroupBySubFlow_ElementsPreserved verifies no element is lost or duplicated
// when using the streaming GroupBySubFlow implementation.
func TestGroupBySubFlow_ElementsPreserved(t *testing.T) {
	const n = 50
	elems := make([]int, n)
	for i := range elems {
		elems[i] = i
	}
	src := stream.FromSlice(elems)

	merged := stream.GroupBySubFlow(src, 5, func(v int) string {
		return string(rune('A' + v%5)) // keys A–E
	}).MergeSubstreams()

	result, err := stream.RunWith(merged, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != n {
		t.Fatalf("expected %d elements, got %d", n, len(result))
	}

	seen := make(map[int]int)
	for _, v := range result {
		seen[v]++
	}
	for i := range elems {
		if seen[i] != 1 {
			t.Errorf("element %d appeared %d times", i, seen[i])
		}
	}
}

/*
 * flow_test.go
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

// ─── GroupBy ──────────────────────────────────────────────────────────────

// TestGroupBy_BasicGrouping verifies that elements with the same key end up in
// the same sub-stream and that each sub-stream preserves upstream order.
func TestGroupBy_BasicGrouping(t *testing.T) {
	src := stream.FromSlice([]string{"apple", "ant", "banana", "avocado", "blueberry"})

	substreams, err := stream.RunWith(
		stream.GroupBy(src, 10, func(s string) string { return string(s[0]) }),
		stream.Collect[stream.SubStream[string]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Expect two distinct keys: "a" and "b".
	if len(substreams) != 2 {
		t.Fatalf("got %d substreams, want 2", len(substreams))
	}

	byKey := make(map[string][]string)
	for _, ss := range substreams {
		elems, err := stream.RunWith(ss.Source, stream.Collect[string](), stream.ActorMaterializer{})
		if err != nil {
			t.Fatalf("substream %q: unexpected error: %v", ss.Key, err)
		}
		byKey[ss.Key] = elems
	}

	wantA := []string{"apple", "ant", "avocado"}
	wantB := []string{"banana", "blueberry"}

	if len(byKey["a"]) != len(wantA) {
		t.Fatalf("key 'a': got %v, want %v", byKey["a"], wantA)
	}
	for i, v := range byKey["a"] {
		if v != wantA[i] {
			t.Fatalf("key 'a' index %d: got %q, want %q", i, v, wantA[i])
		}
	}

	if len(byKey["b"]) != len(wantB) {
		t.Fatalf("key 'b': got %v, want %v", byKey["b"], wantB)
	}
	for i, v := range byKey["b"] {
		if v != wantB[i] {
			t.Fatalf("key 'b' index %d: got %q, want %q", i, v, wantB[i])
		}
	}
}

// TestGroupBy_SingleKey verifies that all elements end up in one sub-stream
// when every element maps to the same key.
func TestGroupBy_SingleKey(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5})

	substreams, err := stream.RunWith(
		stream.GroupBy(src, 1, func(int) string { return "all" }),
		stream.Collect[stream.SubStream[int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(substreams) != 1 {
		t.Fatalf("got %d substreams, want 1", len(substreams))
	}
	if substreams[0].Key != "all" {
		t.Fatalf("got key %q, want \"all\"", substreams[0].Key)
	}

	elems, err := stream.RunWith(substreams[0].Source, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("substream: unexpected error: %v", err)
	}
	if len(elems) != 5 {
		t.Fatalf("got %d elements, want 5", len(elems))
	}
}

// TestGroupBy_KeysInInsertionOrder verifies that sub-streams are emitted in
// the order their keys first appear in the upstream.
func TestGroupBy_KeysInInsertionOrder(t *testing.T) {
	// keys appear in order: c, a, b
	src := stream.FromSlice([]string{"cat", "apple", "bat", "car", "ant"})

	substreams, err := stream.RunWith(
		stream.GroupBy(src, 10, func(s string) string { return string(s[0]) }),
		stream.Collect[stream.SubStream[string]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantOrder := []string{"c", "a", "b"}
	if len(substreams) != len(wantOrder) {
		t.Fatalf("got %d substreams, want %d", len(substreams), len(wantOrder))
	}
	for i, ss := range substreams {
		if ss.Key != wantOrder[i] {
			t.Fatalf("substream %d: got key %q, want %q", i, ss.Key, wantOrder[i])
		}
	}
}

// TestGroupBy_MaxSubstreamsExceeded verifies that the stream fails with
// ErrTooManySubstreams when distinct keys exceed the configured limit.
func TestGroupBy_MaxSubstreamsExceeded(t *testing.T) {
	src := stream.FromSlice([]string{"apple", "banana", "cherry"}) // 3 distinct keys

	_, err := stream.RunWith(
		stream.GroupBy(src, 2, func(s string) string { return string(s[0]) }),
		stream.Collect[stream.SubStream[string]](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected ErrTooManySubstreams, got nil")
	}
	if !errors.Is(err, stream.ErrTooManySubstreams) {
		t.Fatalf("expected ErrTooManySubstreams, got %v", err)
	}
}

// TestGroupBy_UpstreamErrorPropagates verifies that an error from the source
// is surfaced by the GroupBy operator.
func TestGroupBy_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("upstream failure")

	_, err := stream.RunWith(
		stream.GroupBy(stream.Failed[string](sentinelErr), 10, func(s string) string { return s }),
		stream.Collect[stream.SubStream[string]](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

// TestGroupBy_TotalElementsPreserved verifies that every element from the
// upstream appears in exactly one sub-stream (no duplication, no omission).
func TestGroupBy_TotalElementsPreserved(t *testing.T) {
	const total = 50
	src := stream.FromSlice(makeRange(total))

	substreams, err := stream.RunWith(
		stream.GroupBy(src, 10, func(n int) string {
			return string(rune('A' + n%5)) // 5 keys: A, B, C, D, E
		}),
		stream.Collect[stream.SubStream[int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(substreams) != 5 {
		t.Fatalf("got %d substreams, want 5", len(substreams))
	}

	var allElems []int
	for _, ss := range substreams {
		elems, err := stream.RunWith(ss.Source, stream.Collect[int](), stream.ActorMaterializer{})
		if err != nil {
			t.Fatalf("substream %q: unexpected error: %v", ss.Key, err)
		}
		allElems = append(allElems, elems...)
	}

	sort.Ints(allElems)
	if len(allElems) != total {
		t.Fatalf("got %d elements total, want %d", len(allElems), total)
	}
	for i, v := range allElems {
		if v != i {
			t.Fatalf("index %d: got %d, want %d (element missing or duplicated)", i, v, i)
		}
	}
}

// TestGroupBy_WithVia verifies that GroupBy composes with Via correctly:
// a flow transformation is applied before grouping.
func TestGroupBy_WithVia(t *testing.T) {
	src := stream.FromSlice([]int{1, -2, 3, -4, 5})
	// Map to absolute value first, then group by odd/even.
	mapped := stream.Via(src, stream.Map(func(n int) int {
		if n < 0 {
			return -n
		}
		return n
	}))

	substreams, err := stream.RunWith(
		stream.GroupBy(mapped, 2, func(n int) string {
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

	byKey := make(map[string][]int)
	for _, ss := range substreams {
		elems, err := stream.RunWith(ss.Source, stream.Collect[int](), stream.ActorMaterializer{})
		if err != nil {
			t.Fatalf("substream %q: unexpected error: %v", ss.Key, err)
		}
		byKey[ss.Key] = elems
	}

	// odd: 1, 3, 5  even: 2, 4
	if len(byKey["odd"]) != 3 {
		t.Fatalf("odd: got %v, want [1 3 5]", byKey["odd"])
	}
	if len(byKey["even"]) != 2 {
		t.Fatalf("even: got %v, want [2 4]", byKey["even"])
	}
}

// ─── Log ──────────────────────────────────────────────────────────────────

// TestLog_PassThrough verifies that Log forwards all elements unchanged and
// does not affect the element count or values.
func TestLog_PassThrough(t *testing.T) {
	const N = 10

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice(makeRange(N)),
			stream.Map(func(n int) int { return n * 2 }).Log("double"),
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

// TestLog_ErrorPassThrough verifies that Log forwards errors without
// swallowing or wrapping them.
func TestLog_ErrorPassThrough(t *testing.T) {
	sentinelErr := errors.New("log test error")

	_, err := stream.RunWith(
		stream.Via(
			stream.Failed[int](sentinelErr),
			stream.Map(func(n int) int { return n }).Log("fail-path"),
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

// TestLog_PreservesMatValue verifies that Log does not affect the upstream
// materialized value when chained after another Flow.
func TestLog_PreservesMatValue(t *testing.T) {
	// Map with identity then Log — result must still be all elements in order.
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]string{"hello", "world"}),
			stream.Map(func(s string) string { return s }).Log("strings"),
		),
		stream.Collect[string](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"hello", "world"}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %q, want %q", i, v, want[i])
		}
	}
}

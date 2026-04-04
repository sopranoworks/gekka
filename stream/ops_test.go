/*
 * ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func collectInts(t *testing.T, src stream.Source[int, stream.NotUsed]) []int {
	t.Helper()
	var out []int
	graph := src.To(stream.Foreach(func(n int) { out = append(out, n) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	return out
}

func collectStrings(t *testing.T, src stream.Source[string, stream.NotUsed]) []string {
	t.Helper()
	var out []string
	graph := src.To(stream.Foreach(func(s string) { out = append(out, s) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	return out
}

// ── TakeWhile ─────────────────────────────────────────────────────────────────

func TestTakeWhile_Basic(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}),
		stream.TakeWhile(func(n int) bool { return n < 4 }))
	got := collectInts(t, src)
	want := []int{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("TakeWhile: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("TakeWhile[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestTakeWhile_AllMatch(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3}),
		stream.TakeWhile(func(n int) bool { return true }))
	got := collectInts(t, src)
	if len(got) != 3 {
		t.Errorf("TakeWhile all-match: got %v", got)
	}
}

func TestTakeWhile_NoneMatch(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3}),
		stream.TakeWhile(func(n int) bool { return false }))
	got := collectInts(t, src)
	if len(got) != 0 {
		t.Errorf("TakeWhile none-match: got %v", got)
	}
}

func TestTakeWhile_Method(t *testing.T) {
	flow := stream.Map(func(n int) int { return n * 2 }).
		TakeWhile(func(n int) bool { return n < 8 })
	src := stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}), flow)
	got := collectInts(t, src)
	want := []int{2, 4, 6}
	if len(got) != len(want) {
		t.Fatalf("TakeWhile method: got %v, want %v", got, want)
	}
}

// ── DropWhile ─────────────────────────────────────────────────────────────────

func TestDropWhile_Basic(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}),
		stream.DropWhile(func(n int) bool { return n < 3 }))
	got := collectInts(t, src)
	want := []int{3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("DropWhile: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("DropWhile[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestDropWhile_AllDrop(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3}),
		stream.DropWhile(func(n int) bool { return true }))
	got := collectInts(t, src)
	if len(got) != 0 {
		t.Errorf("DropWhile all-drop: got %v", got)
	}
}

func TestDropWhile_NoneDrop(t *testing.T) {
	src := stream.Via(stream.FromSlice([]int{1, 2, 3}),
		stream.DropWhile(func(n int) bool { return false }))
	got := collectInts(t, src)
	if len(got) != 3 {
		t.Errorf("DropWhile none-drop: got %v", got)
	}
}

func TestDropWhile_OnlyDropsPrefix(t *testing.T) {
	// Elements after the first false must never be dropped even if pred would
	// return true for them.
	src := stream.Via(stream.FromSlice([]int{1, 3, 2, 4}),
		stream.DropWhile(func(n int) bool { return n%2 != 0 }))
	got := collectInts(t, src)
	want := []int{2, 4}
	if len(got) != len(want) {
		t.Fatalf("DropWhile prefix-only: got %v, want %v", got, want)
	}
}

// ── FlatMapConcat ─────────────────────────────────────────────────────────────

func TestFlatMapConcat_Basic(t *testing.T) {
	// Each element n expands to [n, n*10].
	src := stream.FlatMapConcat(
		stream.FromSlice([]int{1, 2, 3}),
		func(n int) stream.Source[int, stream.NotUsed] {
			return stream.FromSlice([]int{n, n * 10})
		},
	)
	got := collectInts(t, src)
	want := []int{1, 10, 2, 20, 3, 30}
	if len(got) != len(want) {
		t.Fatalf("FlatMapConcat: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("FlatMapConcat[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestFlatMapConcat_Sequential(t *testing.T) {
	// FlatMapConcat must process sub-sources sequentially (breadth=1).
	// We verify ordering: all of source-1 before any of source-2.
	var mu sync.Mutex
	var order []string
	record := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }

	src := stream.FlatMapConcat(
		stream.FromSlice([]string{"A", "B"}),
		func(prefix string) stream.Source[string, stream.NotUsed] {
			return stream.FromSlice([]string{prefix + "1", prefix + "2", prefix + "3"})
		},
	)
	graph := src.To(stream.Foreach(func(s string) { record(s) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	want := []string{"A1", "A2", "A3", "B1", "B2", "B3"}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("FlatMapConcat order[%d] = %q, want %q", i, order[i], w)
		}
	}
}

func TestFlatMapConcat_EmptySubSource(t *testing.T) {
	src := stream.FlatMapConcat(
		stream.FromSlice([]int{1, 2, 3}),
		func(n int) stream.Source[int, stream.NotUsed] {
			if n == 2 {
				return stream.FromSlice([]int{}) // empty
			}
			return stream.FromSlice([]int{n})
		},
	)
	got := collectInts(t, src)
	want := []int{1, 3}
	if len(got) != len(want) {
		t.Fatalf("FlatMapConcat empty sub-source: got %v, want %v", got, want)
	}
}

// ── MapAsyncUnordered ─────────────────────────────────────────────────────────

func TestMapAsyncUnordered_AllResults(t *testing.T) {
	fn := func(n int) <-chan int {
		ch := make(chan int, 1)
		go func() {
			// Simulate varying latency — slower items should not block faster ones.
			time.Sleep(time.Duration(5-n) * time.Millisecond)
			ch <- n * 2
			close(ch)
		}()
		return ch
	}

	src := stream.MapAsyncUnordered(stream.FromSlice([]int{1, 2, 3, 4, 5}), 4, fn)
	got := collectInts(t, src)

	if len(got) != 5 {
		t.Fatalf("MapAsyncUnordered: got %d results, want 5; values=%v", len(got), got)
	}

	sort.Ints(got)
	want := []int{2, 4, 6, 8, 10}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("MapAsyncUnordered sorted[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestMapAsyncUnordered_DropOnClosedChannel(t *testing.T) {
	// Channels closed without a value should be dropped (not panic).
	fn := func(n int) <-chan int {
		ch := make(chan int, 1)
		go func() {
			if n%2 == 0 {
				ch <- n
			}
			close(ch) // odd: close without value
		}()
		return ch
	}

	src := stream.MapAsyncUnordered(stream.FromSlice([]int{1, 2, 3, 4}), 4, fn)
	got := collectInts(t, src)

	sort.Ints(got)
	want := []int{2, 4}
	if len(got) != len(want) {
		t.Fatalf("MapAsyncUnordered drop: got %v, want %v", got, want)
	}
}

// ── Scan ──────────────────────────────────────────────────────────────────────

func TestScan_RunningSum(t *testing.T) {
	src := stream.Scan(stream.FromSlice([]int{1, 2, 3, 4}), 0, func(acc, n int) int {
		return acc + n
	})
	got := collectInts(t, src)
	want := []int{0, 1, 3, 6, 10}
	if len(got) != len(want) {
		t.Fatalf("Scan running sum: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Scan[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestScan_EmitsSeedOnEmpty(t *testing.T) {
	src := stream.Scan(stream.FromSlice([]int{}), 42, func(acc, n int) int {
		return acc + n
	})
	got := collectInts(t, src)
	// Even with empty input, the seed must be emitted.
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("Scan empty: got %v, want [42]", got)
	}
}

func TestScan_StringConcat(t *testing.T) {
	src := stream.Scan(stream.FromSlice([]string{"a", "b", "c"}), "", func(acc, s string) string {
		return acc + s
	})
	got := collectStrings(t, src)
	want := []string{"", "a", "ab", "abc"}
	if len(got) != len(want) {
		t.Fatalf("Scan string: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Scan string[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

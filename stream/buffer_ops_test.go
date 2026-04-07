/*
 * buffer_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/stream"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectAny[T any](t *testing.T, src stream.Source[T, stream.NotUsed]) []T {
	t.Helper()
	var out []T
	graph := src.To(stream.Foreach(func(v T) { out = append(out, v) }))
	_, err := graph.Run(stream.SyncMaterializer{})
	require.NoError(t, err)
	return out
}

// ─── Batch tests ──────────────────────────────────────────────────────────

func TestBatch_BasicAggregation(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5})
	batched := stream.Via(src, stream.Batch[int, int](
		3,                                         // max weight
		func(i int) int64 { return 1 },            // each element costs 1
		func(i int) int { return i },              // seed
		func(acc, i int) int { return acc + i },   // aggregate
	))

	got := collectAny(t, batched)
	// With max weight 3, we get batches of up to 3 elements summed
	sum := 0
	for _, v := range got {
		sum += v
	}
	assert.Equal(t, 15, sum)
}

func TestBatch_SingleElementBatches(t *testing.T) {
	src := stream.FromSlice([]int{10, 20, 30})
	batched := stream.Via(src, stream.Batch[int, int](
		1,
		func(i int) int64 { return 1 },
		func(i int) int { return i },
		func(acc, i int) int { return acc + i },
	))

	got := collectAny(t, batched)
	assert.Equal(t, []int{10, 20, 30}, got)
}

func TestBatch_Empty(t *testing.T) {
	src := stream.FromSlice([]int{})
	batched := stream.Via(src, stream.Batch[int, int](
		10,
		func(i int) int64 { return 1 },
		func(i int) int { return i },
		func(acc, i int) int { return acc + i },
	))

	got := collectAny(t, batched)
	assert.Empty(t, got)
}

func TestBatch_WeightedCost(t *testing.T) {
	// Elements with varying cost
	src := stream.FromSlice([]int{1, 2, 3, 4})
	batched := stream.Via(src, stream.Batch[int, []int](
		5,                                                          // max weight
		func(i int) int64 { return int64(i) },                     // cost = element value
		func(i int) []int { return []int{i} },                     // seed
		func(acc []int, i int) []int { return append(acc, i) },    // aggregate
	))

	got := collectAny(t, batched)
	// 1+2=3 (ok), 3+3=6>5 so batch [1,2] emitted, then 3+4=7>5 so [3] emitted, then [4]
	assert.Len(t, got, 3)
}

// ─── Expand tests ─────────────────────────────────────────────────────────

func TestExpand_PassThrough(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})
	expanded := stream.Via(src, stream.Expand(func(i int) func() (int, bool) {
		return func() (int, bool) { return i, true }
	}))
	got := collectAny(t, expanded)
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestExpand_SourceMethod(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3}).Expand(func(i int) func() (int, bool) {
		return func() (int, bool) { return i, true }
	})
	got := collectAny(t, src)
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestExtrapolate_WithInitial(t *testing.T) {
	src := stream.FromSlice([]int{10, 20})
	extra := stream.Via(src, stream.Extrapolate(func(i int) func() (int, bool) {
		return func() (int, bool) { return i, true }
	}, 0))
	got := collectAny(t, extra)
	assert.Equal(t, []int{0, 10, 20}, got)
}

// ─── Sliding tests ────────────────────────────────────────────────────────

func TestSliding_Basic(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5})
	slid := stream.Via(src, stream.Sliding[int](3, 1))
	got := collectAny(t, slid)
	assert.Equal(t, [][]int{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}}, got)
}

func TestSliding_StepTwo(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})
	slid := stream.Via(src, stream.Sliding[int](2, 2))
	got := collectAny(t, slid)
	assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5, 6}}, got)
}

func TestSliding_WindowLargerThanInput(t *testing.T) {
	src := stream.FromSlice([]int{1, 2})
	slid := stream.Via(src, stream.Sliding[int](5, 1))
	got := collectAny(t, slid)
	assert.Len(t, got, 1)
	assert.Equal(t, []int{1, 2}, got[0])
}

func TestSliding_Empty(t *testing.T) {
	src := stream.FromSlice([]int{})
	slid := stream.Via(src, stream.Sliding[int](3, 1))
	got := collectAny(t, slid)
	assert.Empty(t, got)
}

// ─── SplitWhen / SplitAfter tests ─────────────────────────────────────────

func TestSplitWhen_Basic(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})
	sf := stream.SplitWhen(src, 10, func(i int) bool { return i%2 == 0 })
	got := collectAny(t, sf.MergeSubstreams())

	sum := 0
	for _, v := range got {
		sum += v
	}
	assert.Equal(t, 21, sum)
	assert.Len(t, got, 6)
}

func TestSplitAfter_Basic(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})
	sf := stream.SplitAfter(src, 10, func(i int) bool { return i%2 == 0 })
	got := collectAny(t, sf.MergeSubstreams())

	sum := 0
	for _, v := range got {
		sum += v
	}
	assert.Equal(t, 21, sum)
	assert.Len(t, got, 6)
}

func TestSplitWhen_Empty(t *testing.T) {
	src := stream.FromSlice([]int{})
	sf := stream.SplitWhen(src, 10, func(i int) bool { return true })
	got := collectAny(t, sf.MergeSubstreams())
	assert.Empty(t, got)
}

// ─── ScanAsync tests ──────────────────────────────────────────────────────

func TestScanAsync_Basic(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})
	scanned := stream.Via(src, stream.ScanAsync(0, func(acc int, elem int) <-chan int {
		ch := make(chan int, 1)
		ch <- acc + elem
		return ch
	}))

	got := collectAny(t, scanned)
	assert.Equal(t, []int{0, 1, 3, 6}, got)
}

func TestScanAsync_Empty(t *testing.T) {
	src := stream.FromSlice([]int{})
	scanned := stream.Via(src, stream.ScanAsync(42, func(acc int, elem int) <-chan int {
		ch := make(chan int, 1)
		ch <- acc + elem
		return ch
	}))

	got := collectAny(t, scanned)
	assert.Equal(t, []int{42}, got)
}

// ─── Reactive Streams interop tests ───────────────────────────────────────

type testPublisher struct {
	elems []int
}

func (p *testPublisher) Subscribe(sub stream.Subscriber[int]) {
	sub.OnSubscribe(&testSubscription{elems: p.elems, sub: sub})
}

type testSubscription struct {
	elems []int
	sub   stream.Subscriber[int]
	pos   int
	mu    sync.Mutex
}

func (s *testSubscription) Request(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := int64(0); i < n && s.pos < len(s.elems); i++ {
		s.sub.OnNext(s.elems[s.pos])
		s.pos++
	}
	if s.pos >= len(s.elems) {
		s.sub.OnComplete()
	}
}

func (s *testSubscription) Cancel() {}

func TestFromPublisher_Basic(t *testing.T) {
	pub := &testPublisher{elems: []int{10, 20, 30}}
	src := stream.FromPublisher[int](pub)
	got := collectAny(t, src)
	assert.Equal(t, []int{10, 20, 30}, got)
}

func TestFromPublisher_Empty(t *testing.T) {
	pub := &testPublisher{elems: []int{}}
	src := stream.FromPublisher[int](pub)
	got := collectAny(t, src)
	assert.Empty(t, got)
}

type errorPublisher struct {
	err error
}

func (p *errorPublisher) Subscribe(sub stream.Subscriber[int]) {
	sub.OnSubscribe(&errorSubscription{sub: sub, err: p.err})
}

type errorSubscription struct {
	sub stream.Subscriber[int]
	err error
}

func (s *errorSubscription) Request(n int64) {
	s.sub.OnError(s.err)
}

func (s *errorSubscription) Cancel() {}

func TestFromPublisher_Error(t *testing.T) {
	expectedErr := errors.New("test error")
	pub := &errorPublisher{err: expectedErr}
	src := stream.FromPublisher[int](pub)

	graph := src.To(stream.ForeachErr(func(v int) error { return nil }))
	_, gotErr := graph.Run(stream.SyncMaterializer{})
	assert.ErrorIs(t, gotErr, expectedErr)
}

func TestAsPublisher_RoundTrip(t *testing.T) {
	original := []int{5, 10, 15, 20}
	src := stream.FromSlice(original)
	pub := stream.AsPublisher(src)
	roundTripped := stream.FromPublisher[int](pub)
	got := collectAny(t, roundTripped)
	assert.Equal(t, original, got)
}

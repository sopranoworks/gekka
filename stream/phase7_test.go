/*
 * phase7_test.go
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

	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P7-A: WireTap ────────────────────────────────────────────────────────

func TestWireTap_ForwardsUnchanged(t *testing.T) {
	tapCh := make(chan int, 10)

	src := stream.FromSlice([]int{1, 2, 3})
	result, err := stream.RunWith(
		stream.Via(src, stream.WireTap(func(v int) { tapCh <- v })),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, result)

	var tapResults []int
	for i := 0; i < 3; i++ {
		tapResults = append(tapResults, <-tapCh)
	}
	sort.Ints(tapResults)
	assert.Equal(t, []int{1, 2, 3}, tapResults)
}

func TestWireTap_DoesNotBlockMainStream(t *testing.T) {
	// The tap uses a WaitGroup to verify goroutines fire asynchronously;
	// the main stream must complete before wg.Wait() unblocks.
	var wg sync.WaitGroup
	wg.Add(5)
	var mu sync.Mutex
	var tapSeen []int

	src := stream.FromSlice([]int{10, 20, 30, 40, 50})
	result, err := stream.RunWith(
		stream.Via(src, stream.WireTap(func(v int) {
			defer wg.Done()
			mu.Lock()
			tapSeen = append(tapSeen, v)
			mu.Unlock()
		})),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 20, 30, 40, 50}, result)

	wg.Wait()
	sort.Ints(tapSeen)
	assert.Equal(t, []int{10, 20, 30, 40, 50}, tapSeen)
}

// ─── P7-B: Interleave ─────────────────────────────────────────────────────

func TestInterleave_EqualLengthSources(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})
	other := stream.FromSlice([]int{10, 20, 30, 40, 50, 60})

	result, err := stream.RunWith(
		stream.Via(src, stream.Interleave(2, other)),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	// segmentSize=2: take 2 from src, 2 from other, repeat
	assert.Equal(t, []int{1, 2, 10, 20, 3, 4, 30, 40, 5, 6, 50, 60}, result)
}

func TestInterleave_SegmentSizeOne(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3})
	other := stream.FromSlice([]int{100, 200, 300})

	result, err := stream.RunWith(
		stream.Via(src, stream.Interleave(1, other)),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 100, 2, 200, 3, 300}, result)
}

func TestInterleave_SrcExhaustsFirst(t *testing.T) {
	src := stream.FromSlice([]int{1, 2})
	other := stream.FromSlice([]int{10, 20, 30, 40})

	result, err := stream.RunWith(
		stream.Via(src, stream.Interleave(2, other)),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	// take 2 from src: [1,2], take 2 from other: [10,20], src exhausted → drain other: [30,40]
	assert.Equal(t, []int{1, 2, 10, 20, 30, 40}, result)
}

func TestInterleave_OtherExhaustsFirst(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4})
	other := stream.FromSlice([]int{10, 20})

	result, err := stream.RunWith(
		stream.Via(src, stream.Interleave(2, other)),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	require.NoError(t, err)
	// take 2 from src: [1,2], take 2 from other: [10,20], other exhausted → drain src: [3,4]
	assert.Equal(t, []int{1, 2, 10, 20, 3, 4}, result)
}

// ─── P7-C: MergePrioritized ───────────────────────────────────────────────

func TestMergePrioritized_WeightedDistribution(t *testing.T) {
	b := stream.NewBuilder()

	// src0: priority 3 — 12 distinct values
	src0vals := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	// src1: priority 1 — 4 distinct values (≥100 to distinguish from src0)
	src1vals := []int{100, 200, 300, 400}

	src0 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice(src0vals))
	src1 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice(src1vals))
	merge := stream.Add[stream.FanInShape[int], stream.NotUsed](b, stream.NewMergePrioritized[int]([]int{3, 1}))

	var results []int
	var mu sync.Mutex
	sink := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(v int) {
		mu.Lock()
		results = append(results, v)
		mu.Unlock()
	}))

	stream.Connect(b, src0.Out, merge.Inlets_[0])
	stream.Connect(b, src1.Out, merge.Inlets_[1])
	stream.Connect(b, merge.Out, sink.In)

	g, err := b.Build()
	require.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	require.NoError(t, err)

	assert.Len(t, results, 16, "all 16 elements must be emitted")

	// With weighted round-robin [0,0,0,1] the expected output is:
	// 1,2,3,100, 4,5,6,200, 7,8,9,300, 10,11,12,400
	assert.Equal(t, []int{
		1, 2, 3, 100,
		4, 5, 6, 200,
		7, 8, 9, 300,
		10, 11, 12, 400,
	}, results)
}

func TestMergePrioritized_ExhaustedUpstream(t *testing.T) {
	b := stream.NewBuilder()

	// src0 has fewer elements than src1; after src0 is done, src1 is drained.
	src0 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2}))
	src1 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{10, 20, 30, 40, 50, 60}))
	merge := stream.Add[stream.FanInShape[int], stream.NotUsed](b, stream.NewMergePrioritized[int]([]int{1, 1}))

	var results []int
	sink := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(v int) {
		results = append(results, v)
	}))

	stream.Connect(b, src0.Out, merge.Inlets_[0])
	stream.Connect(b, src1.Out, merge.Inlets_[1])
	stream.Connect(b, merge.Out, sink.In)

	g, err := b.Build()
	require.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	require.NoError(t, err)

	assert.Len(t, results, 8, "all 8 elements must be emitted")
	assert.ElementsMatch(t, append([]int{1, 2}, []int{10, 20, 30, 40, 50, 60}...), results)
}

// ─── P7-D: UnzipWith ──────────────────────────────────────────────────────

func TestUnzipWith2_SplitsStream(t *testing.T) {
	b := stream.NewBuilder()

	type Pair = stream.Pair[int, string]
	pairs := []Pair{{First: 1, Second: "a"}, {First: 2, Second: "b"}, {First: 3, Second: "c"}}

	src := stream.Add[stream.SourceShape[Pair], stream.NotUsed](b, stream.FromSlice(pairs))
	unzip := stream.Add[stream.FanOut2Shape[Pair, int, string], stream.NotUsed](b,
		stream.NewUnzipWith2(func(p Pair) (int, string) { return p.First, p.Second }),
	)

	var ints []int
	var strs []string
	sink0 := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(v int) {
		ints = append(ints, v)
	}))
	sink1 := stream.Add[stream.SinkShape[string], stream.Future[stream.NotUsed]](b, stream.Foreach(func(s string) {
		strs = append(strs, s)
	}))

	stream.Connect(b, src.Out, unzip.In)
	stream.Connect(b, unzip.Out0, sink0.In)
	stream.Connect(b, unzip.Out1, sink1.In)

	g, err := b.Build()
	require.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3}, ints)
	assert.Equal(t, []string{"a", "b", "c"}, strs)
}

func TestUnzipWith2_WithTransform(t *testing.T) {
	b := stream.NewBuilder()

	src := stream.Add[stream.SourceShape[int], stream.NotUsed](b,
		stream.FromSlice([]int{1, 2, 3, 4, 5}))
	unzip := stream.Add[stream.FanOut2Shape[int, int, bool], stream.NotUsed](b,
		stream.NewUnzipWith2(func(n int) (int, bool) { return n * n, n%2 == 0 }),
	)

	var squares []int
	var evens []bool
	sink0 := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(v int) {
		squares = append(squares, v)
	}))
	sink1 := stream.Add[stream.SinkShape[bool], stream.Future[stream.NotUsed]](b, stream.Foreach(func(v bool) {
		evens = append(evens, v)
	}))

	stream.Connect(b, src.Out, unzip.In)
	stream.Connect(b, unzip.Out0, sink0.In)
	stream.Connect(b, unzip.Out1, sink1.In)

	g, err := b.Build()
	require.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	require.NoError(t, err)

	assert.Equal(t, []int{1, 4, 9, 16, 25}, squares)
	assert.Equal(t, []bool{false, true, false, true, false}, evens)
}

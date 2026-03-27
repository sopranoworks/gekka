/*
 * junctions_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
)

func TestJunctions_Broadcast(t *testing.T) {
	b := stream.NewBuilder()
	src := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2, 3}))
	bcast := stream.Add[stream.FanOutShape[int], stream.NotUsed](b, stream.NewBroadcast[int](2))
	
	var r1, r2 []int
	var mu sync.Mutex
	
	sink1 := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(i int) {
		mu.Lock()
		r1 = append(r1, i)
		mu.Unlock()
	}))
	
	sink2 := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(i int) {
		mu.Lock()
		r2 = append(r2, i)
		mu.Unlock()
	}))

	stream.Connect(b, src.Out, bcast.In)
	stream.Connect(b, bcast.Outlets_[0], sink1.In)
	stream.Connect(b, bcast.Outlets_[1], sink2.In)

	g, err := b.Build()
	assert.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	assert.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3}, r1)
	assert.Equal(t, []int{1, 2, 3}, r2)
}

func TestJunctions_Merge(t *testing.T) {
	b := stream.NewBuilder()
	src1 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2}))
	src2 := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{3, 4}))
	merge := stream.Add[stream.FanInShape[int], stream.NotUsed](b, stream.NewMerge[int](2))
	
	var res []int
	var mu sync.Mutex
	sink := stream.Add[stream.SinkShape[int], stream.Future[stream.NotUsed]](b, stream.Foreach(func(i int) {
		mu.Lock()
		res = append(res, i)
		mu.Unlock()
	}))

	stream.Connect(b, src1.Out, merge.Inlets_[0])
	stream.Connect(b, src2.Out, merge.Inlets_[1])
	stream.Connect(b, merge.Out, sink.In)

	g, err := b.Build()
	assert.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	assert.NoError(t, err)

	assert.ElementsMatch(t, []int{1, 2, 3, 4}, res)
}

func TestJunctions_Zip(t *testing.T) {
	b := stream.NewBuilder()
	srcA := stream.Add[stream.SourceShape[string], stream.NotUsed](b, stream.FromSlice([]string{"a", "b", "c"}))
	srcB := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2}))
	zip := stream.Add[stream.FanIn2Shape[string, int, stream.Pair[string, int]], stream.NotUsed](b, stream.NewZip[string, int]())
	
	var res []stream.Pair[string, int]
	var mu sync.Mutex
	sink := stream.Add[stream.SinkShape[stream.Pair[string, int]], stream.Future[stream.NotUsed]](b, stream.Foreach(func(p stream.Pair[string, int]) {
		mu.Lock()
		res = append(res, p)
		mu.Unlock()
	}))

	stream.Connect(b, srcA.Out, zip.In0)
	stream.Connect(b, srcB.Out, zip.In1)
	stream.Connect(b, zip.Out, sink.In)

	g, err := b.Build()
	assert.NoError(t, err)

	_, err = g.Run(stream.SyncMaterializer{})
	assert.NoError(t, err)

	assert.Equal(t, 2, len(res))
	assert.Equal(t, "a", res[0].First)
	assert.Equal(t, 1, res[0].Second)
	assert.Equal(t, "b", res[1].First)
	assert.Equal(t, 2, res[1].Second)
}

// Dummy method to extract mat values for the tests above
// Real tests might just use a standard ActorMaterializer resolution or Future based sink.
// I will rewrite this part to strictly use gekka semantics for `RunWith`.

func TestJunctions_Compile(t *testing.T) {
	_ = stream.NewMerge[int](3)
	_ = stream.NewBroadcast[string](2)
	_ = stream.NewZip[float64, int]()
}

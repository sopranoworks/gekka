/*
 * graph_dsl_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

func TestGraphDSL_Simple(t *testing.T) {
	b := stream.NewBuilder()
	
	src := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2, 3}))
	sink := stream.Add[stream.SinkShape[int], []int](b, stream.Collect[int]())
	
	stream.Connect(b, src.Out, sink.In)
	
	g, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	
	// How to get the materialized value from the sink?
	// Currently Build() returns RunnableGraph[NotUsed].
	// This is a limitation of the current design.
	
	_, err = g.Run(stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestGraphDSL_Flow(t *testing.T) {
	b := stream.NewBuilder()

	src := stream.Add[stream.SourceShape[int], stream.NotUsed](b, stream.FromSlice([]int{1, 2, 3}))
	flow := stream.Add[stream.FlowShape[int, int], stream.NotUsed](b, stream.Map(func(n int) int { return n * 2 }))
	sink := stream.Add[stream.SinkShape[int], []int](b, stream.Collect[int]())

	stream.Connect(b, src.Out, flow.In)
	stream.Connect(b, flow.Out, sink.In)

	g, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	_, err = g.Run(stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

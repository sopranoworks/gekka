/*
 * basic_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

func TestSource_FromSlice_Foreach(t *testing.T) {
	var got []int
	graph := stream.FromSlice([]int{1, 2, 3}).To(stream.Foreach(func(n int) {
		got = append(got, n)
	}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestSource_Via_Map(t *testing.T) {
	src := stream.FromSlice([]int{1, 2, 3, 4})
	doubled := stream.Via(src, stream.Map(func(n int) int { return n * 2 }))

	result, err := stream.RunWith(doubled, stream.Collect[int](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{2, 4, 6, 8}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, result[i], want[i])
		}
	}
}

func TestSource_Filter(t *testing.T) {
	result, err := stream.RunWith(
		stream.FromSlice([]int{1, 2, 3, 4, 5}).Filter(func(n int) bool { return n%2 == 0 }),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{2, 4}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, result[i], want[i])
		}
	}
}

func TestSource_Take(t *testing.T) {
	result, err := stream.RunWith(
		stream.FromSlice([]int{10, 20, 30, 40, 50}).Take(3),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{10, 20, 30}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, result[i], want[i])
		}
	}
}

func TestPipeline_Ordering(t *testing.T) {
	// Verifies strict FIFO ordering through multiple transforms.
	src := stream.FromSlice([]int{5, 3, 1, 4, 2})
	withIndex := stream.Via(src, stream.Map(func(n int) string {
		return string(rune('a' + n - 1))
	}))
	result, err := stream.RunWith(withIndex, stream.Collect[string](), stream.SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"e", "c", "a", "d", "b"}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("index %d: got %q, want %q", i, result[i], want[i])
		}
	}
}

func TestSource_Failed_PropagatesError(t *testing.T) {
	sentinel := errors.New("source error")
	graph := stream.Failed[int](sentinel).To(stream.Ignore[int]())
	_, err := graph.Run(stream.SyncMaterializer{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

func TestSink_Head(t *testing.T) {
	head, err := stream.RunWith(
		stream.FromSlice([]int{7, 8, 9}),
		stream.Head[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if head == nil || *head != 7 {
		t.Fatalf("expected 7, got %v", head)
	}
}

func TestSink_Head_Empty(t *testing.T) {
	head, err := stream.RunWith(
		stream.FromSlice([]int{}),
		stream.Head[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if head != nil {
		t.Fatalf("expected nil for empty source, got %v", head)
	}
}

func TestRunnableGraph_MustRun(t *testing.T) {
	graph := stream.FromSlice([]int{1, 2, 3}).To(stream.Ignore[int]())
	// Should not panic.
	graph.MustRun(stream.SyncMaterializer{})
}

func TestFlow_Chained(t *testing.T) {
	// Map then Filter via ViaFlow.
	double := stream.Map(func(n int) int { return n * 2 })
	keepBig := stream.Filter(func(n int) bool { return n > 5 })
	combined := stream.ViaFlow(double, keepBig)

	src := stream.FromSlice([]int{1, 2, 3, 4, 5})
	result, err := stream.RunWith(
		stream.Via(src, combined),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1*2=2(drop), 2*2=4(drop), 3*2=6(keep), 4*2=8(keep), 5*2=10(keep)
	want := []int{6, 8, 10}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, result[i], want[i])
		}
	}
}

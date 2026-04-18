/*
 * merge_sorted_test.go
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

func intLess(a, b int) bool { return a < b }

func TestMergeSorted_TwoSources(t *testing.T) {
	result, err := stream.RunWith(
		stream.MergeSorted(intLess,
			stream.FromSlice([]int{1, 3, 5}),
			stream.FromSlice([]int{2, 4, 6}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 3, 4, 5, 6}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("elem[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestMergeSorted_NSources(t *testing.T) {
	result, err := stream.RunWith(
		stream.MergeSorted(intLess,
			stream.FromSlice([]int{1, 4, 7}),
			stream.FromSlice([]int{2, 5, 8}),
			stream.FromSlice([]int{3, 6, 9}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("elem[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestMergeSorted_OneEmpty(t *testing.T) {
	result, err := stream.RunWith(
		stream.MergeSorted(intLess,
			stream.Empty[int](),
			stream.FromSlice([]int{1, 2}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("elem[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestMergeSorted_AllEmpty(t *testing.T) {
	result, err := stream.RunWith(
		stream.MergeSorted(intLess,
			stream.Empty[int](),
			stream.Empty[int](),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 elements, got %d: %v", len(result), result)
	}
}

func TestMergeSorted_SingleSource(t *testing.T) {
	result, err := stream.RunWith(
		stream.MergeSorted(intLess,
			stream.FromSlice([]int{5, 10, 15}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{5, 10, 15}
	if len(result) != len(expected) {
		t.Fatalf("expected %d elements, got %d: %v", len(expected), len(result), result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("elem[%d]: expected %d, got %d", i, expected[i], v)
		}
	}
}

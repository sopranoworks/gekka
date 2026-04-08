/*
 * source_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"sort"
	"testing"
)

func TestCycle_Take7(t *testing.T) {
	src := Cycle([]int{1, 2, 3}).Take(7)
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 3, 1, 2, 3, 1}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestCycle_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty elements")
		}
	}()
	Cycle([]int{})
}

func TestZipWithIndex(t *testing.T) {
	src := ZipWithIndex(FromSlice([]string{"a", "b", "c"}))
	result, err := RunWith(src, Collect[Pair[string, int64]](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	for i, p := range result {
		if p.Second != int64(i) {
			t.Errorf("index %d: got %d", i, p.Second)
		}
	}
	if result[0].First != "a" || result[1].First != "b" || result[2].First != "c" {
		t.Errorf("elements mismatch: %v", result)
	}
}

func TestCombine_Merge(t *testing.T) {
	s1 := FromSlice([]int{1, 2})
	s2 := FromSlice([]int{3, 4})
	s3 := FromSlice([]int{5, 6})
	src := Combine(CombineMerge, s1, s2, s3)
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(result))
	}
	sort.Ints(result)
	for i, v := range result {
		if v != i+1 {
			t.Errorf("result[%d] = %d, want %d", i, v, i+1)
		}
	}
}

func TestCombine_Concat(t *testing.T) {
	s1 := FromSlice([]int{1, 2})
	s2 := FromSlice([]int{3, 4})
	s3 := FromSlice([]int{5, 6})
	src := Combine(CombineConcat, s1, s2, s3)
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 3, 4, 5, 6}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

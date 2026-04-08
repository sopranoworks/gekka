/*
 * sink_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"testing"
)

func TestFold_Sum(t *testing.T) {
	src := FromSlice([]int{1, 2, 3, 4, 5})
	result, err := RunWith(src, Fold(0, func(acc, x int) int { return acc + x }), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 15 {
		t.Errorf("got %d, want 15", result)
	}
}

func TestFold_EmptySource(t *testing.T) {
	src := FromSlice([]int{})
	result, err := RunWith(src, Fold(42, func(acc, x int) int { return acc + x }), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Errorf("got %d, want 42 (zero value)", result)
	}
}

func TestReduce_Max(t *testing.T) {
	src := FromSlice([]int{3, 1, 4, 1, 5, 9, 2, 6})
	result, err := RunWith(src, Reduce(func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 9 {
		t.Errorf("got %d, want 9", result)
	}
}

func TestReduce_EmptyStream(t *testing.T) {
	src := FromSlice([]int{})
	_, err := RunWith(src, Reduce(func(a, b int) int { return a + b }), SyncMaterializer{})
	if !errors.Is(err, ErrEmptyStream) {
		t.Errorf("expected ErrEmptyStream, got %v", err)
	}
}

func TestLast(t *testing.T) {
	src := FromSlice([]int{1, 2, 3})
	result, err := RunWith(src, Last[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 3 {
		t.Errorf("got %d, want 3", result)
	}
}

func TestLast_Empty(t *testing.T) {
	src := FromSlice([]int{})
	_, err := RunWith(src, Last[int](), SyncMaterializer{})
	if !errors.Is(err, ErrEmptyStream) {
		t.Errorf("expected ErrEmptyStream, got %v", err)
	}
}

func TestLastOption(t *testing.T) {
	src := FromSlice([]int{10, 20, 30})
	result, err := RunWith(src, LastOption[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || *result != 30 {
		t.Errorf("got %v, want *30", result)
	}
}

func TestLastOption_Empty(t *testing.T) {
	src := FromSlice([]int{})
	result, err := RunWith(src, LastOption[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("got %v, want nil", result)
	}
}

func TestHeadOption_NonEmpty(t *testing.T) {
	src := FromSlice([]int{42, 99})
	result, err := RunWith(src, HeadOption[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || *result != 42 {
		t.Errorf("got %v, want *42", result)
	}
}

func TestHeadOption_Empty(t *testing.T) {
	src := FromSlice([]int{})
	result, err := RunWith(src, HeadOption[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("got %v, want nil", result)
	}
}

func TestCancelled(t *testing.T) {
	// Cancelled should not consume any elements
	src := FromSlice([]int{1, 2, 3})
	_, err := RunWith(src, Cancelled[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

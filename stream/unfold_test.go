/*
 * unfold_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import "testing"

func TestUnfold_Fibonacci(t *testing.T) {
	type state struct{ a, b int }
	fib := Unfold(state{0, 1}, func(s state) (int, state, bool) {
		return s.a, state{s.b, s.a + s.b}, true
	}).Take(8)

	result, err := RunWith(fib, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 1, 1, 2, 3, 5, 8, 13}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestUnfold_Termination(t *testing.T) {
	// Count up to 5 then stop
	src := Unfold(1, func(s int) (int, int, bool) {
		if s > 5 {
			return 0, 0, false
		}
		return s, s + 1, true
	})

	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 3, 4, 5}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestUnfoldAsync_Basic(t *testing.T) {
	src := UnfoldAsync(0, func(s int) chan UnfoldResult[int, int] {
		ch := make(chan UnfoldResult[int, int], 1)
		if s >= 3 {
			ch <- UnfoldResult[int, int]{Continue: false}
		} else {
			ch <- UnfoldResult[int, int]{Elem: s * 10, State: s + 1, Continue: true}
		}
		return ch
	})

	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 10, 20}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

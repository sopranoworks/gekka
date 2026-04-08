/*
 * flow_extra_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ─── MapConcat ───────────────────────────────────────────────────────────

func TestMapConcat_Expanding(t *testing.T) {
	src := MapConcat(FromSlice([]int{1, 2, 3}), func(x int) []int {
		out := make([]int, x)
		for i := range out {
			out[i] = x
		}
		return out
	})
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{1, 2, 2, 3, 3, 3}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestMapConcat_EmptyOutput(t *testing.T) {
	src := MapConcat(FromSlice([]int{1, 2, 3}), func(x int) []string {
		if x == 2 {
			return nil // empty for 2
		}
		return []string{strings.Repeat("x", x)}
	})
	result, err := RunWith(src, Collect[string](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []string{"x", "xxx"}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
}

// ─── FlatMapMerge ────────────────────────────────────────────────────────

func TestFlatMapMerge_Concurrent(t *testing.T) {
	src := FlatMapMerge(FromSlice([]int{1, 2, 3}), 3, func(x int) Source[int, NotUsed] {
		return FromSlice([]int{x * 10, x*10 + 1})
	})
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 6 {
		t.Fatalf("expected 6 elements, got %d: %v", len(result), result)
	}
	sort.Ints(result)
	expected := []int{10, 11, 20, 21, 30, 31}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// ─── TakeWithin / DropWithin ─────────────────────────────────────────────

func TestTakeWithin(t *testing.T) {
	// Create a source that emits elements with delays
	src := TakeWithin(FromIteratorFunc(func() func() (int, bool, error) {
		i := 0
		return func() (int, bool, error) {
			i++
			if i > 100 {
				return 0, false, nil
			}
			time.Sleep(10 * time.Millisecond)
			return i, true, nil
		}
	}()), 55*time.Millisecond)

	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should get roughly 4-5 elements within 55ms with 10ms delays
	if len(result) == 0 {
		t.Error("expected at least some elements")
	}
	if len(result) > 10 {
		t.Errorf("expected fewer elements, got %d", len(result))
	}
}

func TestDropWithin(t *testing.T) {
	src := DropWithin(FromIteratorFunc(func() func() (int, bool, error) {
		i := 0
		return func() (int, bool, error) {
			i++
			if i > 10 {
				return 0, false, nil
			}
			time.Sleep(10 * time.Millisecond)
			return i, true, nil
		}
	}()), 35*time.Millisecond)

	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should skip some early elements
	if len(result) == 0 {
		t.Error("expected at least some elements after drop window")
	}
	if len(result) >= 10 {
		t.Errorf("expected some elements dropped, got %d", len(result))
	}
}

// ─── DivertTo / AlsoTo ───────────────────────────────────────────────────

func TestDivertTo(t *testing.T) {
	var diverted []int
	var mu sync.Mutex
	side := Foreach(func(x int) {
		mu.Lock()
		diverted = append(diverted, x)
		mu.Unlock()
	})

	src := DivertTo(FromSlice([]int{1, 2, 3, 4, 5, 6}), side, func(x int) bool {
		return x%2 == 0 // divert even numbers
	})

	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Main output: odd numbers
	expected := []int{1, 3, 5}
	if len(result) != len(expected) {
		t.Fatalf("main got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("main[%d] = %d, want %d", i, v, expected[i])
		}
	}

	// Side output: even numbers
	// Wait a tiny bit for async side to finish
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	expectedSide := []int{2, 4, 6}
	if len(diverted) != len(expectedSide) {
		t.Fatalf("side got %v, want %v", diverted, expectedSide)
	}
	for i, v := range diverted {
		if v != expectedSide[i] {
			t.Errorf("side[%d] = %d, want %d", i, v, expectedSide[i])
		}
	}
}

func TestAlsoTo(t *testing.T) {
	var logged []int
	var mu sync.Mutex
	side := Foreach(func(x int) {
		mu.Lock()
		logged = append(logged, x)
		mu.Unlock()
	})

	src := AlsoTo(FromSlice([]int{1, 2, 3}), side)
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Main should have all elements
	if len(result) != 3 {
		t.Fatalf("main got %v, want [1,2,3]", result)
	}

	// Wait for async side
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(logged) != 3 {
		t.Fatalf("side got %v, want [1,2,3]", logged)
	}
}

// ─── Timeout operators ──────────────────────────────────────────────────

func TestIdleTimeout_OnIdleSource(t *testing.T) {
	// Source that blocks forever after first element
	src := IdleTimeout(FromIteratorFunc(func() func() (int, bool, error) {
		sent := false
		return func() (int, bool, error) {
			if !sent {
				sent = true
				return 1, true, nil
			}
			time.Sleep(5 * time.Second)
			return 0, false, nil
		}
	}()), 50*time.Millisecond)

	_, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if !errors.Is(err, ErrIdleTimeout) {
		t.Errorf("expected ErrIdleTimeout, got %v", err)
	}
}

func TestInitialTimeout_OnSlowSource(t *testing.T) {
	src := InitialTimeout(FromIteratorFunc(func() (int, bool, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, true, nil
	}), 30*time.Millisecond)

	_, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if !errors.Is(err, ErrInitialTimeout) {
		t.Errorf("expected ErrInitialTimeout, got %v", err)
	}
}

func TestKeepAlive_Injects(t *testing.T) {
	// Source that emits 1 then blocks
	callCount := 0
	src := KeepAlive(FromIteratorFunc(func() (int, bool, error) {
		callCount++
		if callCount == 1 {
			return 42, true, nil
		}
		time.Sleep(200 * time.Millisecond)
		return 0, false, nil
	}), 30*time.Millisecond, -1)

	result, _ := RunWith(src.Take(3), Collect[int](), SyncMaterializer{})
	if len(result) < 2 {
		t.Fatalf("expected at least 2 elements, got %v", result)
	}
	if result[0] != 42 {
		t.Errorf("first element = %d, want 42", result[0])
	}
	// Subsequent elements should be injected keepalive values
	for i := 1; i < len(result); i++ {
		if result[i] != -1 {
			t.Errorf("result[%d] = %d, want -1 (injected)", i, result[i])
		}
	}
}

// ─── DelayWith / OrElse / Prepend ────────────────────────────────────────

func TestDelayWith(t *testing.T) {
	start := time.Now()
	src := DelayWith(FromSlice([]int{1, 2}), func(x int) time.Duration {
		return time.Duration(x) * 20 * time.Millisecond
	})
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	if len(result) != 2 {
		t.Fatalf("got %v, want [1,2]", result)
	}
	// Total delay should be at least 20ms+40ms = 60ms
	if elapsed < 50*time.Millisecond {
		t.Errorf("expected at least 50ms delay, got %v", elapsed)
	}
}

func TestOrElse_EmptySource(t *testing.T) {
	src := OrElse(FromSlice([]int{}), FromSlice([]int{99, 100}))
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 || result[0] != 99 || result[1] != 100 {
		t.Errorf("got %v, want [99, 100]", result)
	}
}

func TestOrElse_NonEmptySource(t *testing.T) {
	src := OrElse(FromSlice([]int{1, 2}), FromSlice([]int{99}))
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 || result[0] != 1 || result[1] != 2 {
		t.Errorf("got %v, want [1, 2]", result)
	}
}

func TestPrepend(t *testing.T) {
	src := Prepend(FromSlice([]int{0}), FromSlice([]int{1, 2, 3}))
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 1, 2, 3}
	if len(result) != len(expected) {
		t.Fatalf("got %v, want %v", result, expected)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("result[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// ─── WatchTermination / Monitor ──────────────────────────────────────────

func TestWatchTermination_Complete(t *testing.T) {
	var terminated bool
	var termErr error
	src := WatchTermination(FromSlice([]int{1, 2, 3}), func(err error) {
		terminated = true
		termErr = err
	})
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("got %v, want [1,2,3]", result)
	}
	if !terminated {
		t.Error("callback not fired")
	}
	if termErr != nil {
		t.Errorf("expected nil error, got %v", termErr)
	}
}

func TestWatchTermination_Error(t *testing.T) {
	testErr := errors.New("boom")
	var termErr error
	src := WatchTermination(Failed[int](testErr), func(err error) {
		termErr = err
	})
	_, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if !errors.Is(err, testErr) {
		t.Errorf("expected boom, got %v", err)
	}
	if !errors.Is(termErr, testErr) {
		t.Errorf("callback expected boom, got %v", termErr)
	}
}

func TestMonitor_SeesAllElements(t *testing.T) {
	var seen []int
	src := Monitor(FromSlice([]int{10, 20, 30}), func(x int) {
		seen = append(seen, x)
	})
	result, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("got %v, want [10,20,30]", result)
	}
	if len(seen) != 3 {
		t.Fatalf("monitor saw %v, want [10,20,30]", seen)
	}
	for i, v := range seen {
		if v != result[i] {
			t.Errorf("seen[%d] = %d, want %d", i, v, result[i])
		}
	}
}

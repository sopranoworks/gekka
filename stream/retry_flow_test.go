/*
 * retry_flow_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ── RetryFlowWithBackoff ───────────────────────────────────────────────────────

// TestRetryFlow_SuccessOnFirstAttempt verifies that elements succeeding on the
// first attempt are emitted without any retry.
func TestRetryFlow_SuccessOnFirstAttempt(t *testing.T) {
	flow := stream.MapE(func(n int) (int, error) { return n * 2, nil })

	retrying := stream.RetryFlowWithBackoff(
		stream.RestartSettings{MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, MaxRestarts: 3},
		flow,
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3}), retrying),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{2, 4, 6}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Errorf("[%d] got %d, want %d", i, v, want[i])
		}
	}
}

// TestRetryFlow_SucceedsAfterRetries verifies that an element that fails on the
// first two attempts but succeeds on the third is eventually emitted.
func TestRetryFlow_SucceedsAfterRetries(t *testing.T) {
	var calls atomic.Int32

	flow := stream.MapE(func(n int) (int, error) {
		attempt := int(calls.Add(1))
		if attempt < 3 {
			return 0, errors.New("transient failure")
		}
		return n * 10, nil
	})

	retrying := stream.RetryFlowWithBackoff(
		stream.RestartSettings{
			MinBackoff:  time.Millisecond,
			MaxBackoff:  10 * time.Millisecond,
			MaxRestarts: 5,
		},
		flow,
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{7}), retrying),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error after eventual success: %v", err)
	}
	if len(result) != 1 || result[0] != 70 {
		t.Fatalf("got %v, want [70]", result)
	}
	if calls.Load() != 3 {
		t.Errorf("flow called %d times, want 3", calls.Load())
	}
}

// TestRetryFlow_ExhaustsBudgetPropagatesError verifies that after MaxRestarts
// retries the error propagates downstream.
func TestRetryFlow_ExhaustsBudgetPropagatesError(t *testing.T) {
	sentinel := errors.New("permanent failure")
	var calls atomic.Int32

	flow := stream.MapE(func(n int) (int, error) {
		calls.Add(1)
		return 0, sentinel
	})

	retrying := stream.RetryFlowWithBackoff(
		stream.RestartSettings{
			MinBackoff:  time.Millisecond,
			MaxBackoff:  5 * time.Millisecond,
			MaxRestarts: 2,
		},
		flow,
	)

	_, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1}), retrying),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error after budget exhausted, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("got error %v, want sentinel", err)
	}
	// 1 initial attempt + 2 retries = 3 total calls.
	if calls.Load() != 3 {
		t.Errorf("flow called %d times, want 3", calls.Load())
	}
}

// TestRetryFlow_UnlimitedRetries verifies that MaxRestarts=-1 retries until
// success without a budget cap.
func TestRetryFlow_UnlimitedRetries(t *testing.T) {
	var calls atomic.Int32

	flow := stream.MapE(func(n int) (int, error) {
		c := calls.Add(1)
		if c < 10 {
			return 0, errors.New("not yet")
		}
		return n + 100, nil
	})

	retrying := stream.RetryFlowWithBackoff(
		stream.RestartSettings{
			MinBackoff:  time.Millisecond,
			MaxBackoff:  2 * time.Millisecond,
			MaxRestarts: -1, // unlimited
		},
		flow,
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{5}), retrying),
		stream.Collect[int](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0] != 105 {
		t.Fatalf("got %v, want [105]", result)
	}
}

// TestRetryFlow_MultipleElements verifies that retry semantics apply
// independently to each element: a transient error on one element does not
// affect others.
func TestRetryFlow_MultipleElements(t *testing.T) {
	// Element 2 fails twice then succeeds; elements 1 and 3 always succeed.
	calls := make(map[int]*atomic.Int32)
	for _, k := range []int{1, 2, 3} {
		k := k
		calls[k] = new(atomic.Int32)
		_ = k
	}

	flow := stream.MapE(func(n int) (int, error) {
		c := calls[n].Add(1)
		if n == 2 && c < 3 {
			return 0, errors.New("element 2 not ready")
		}
		return n * 10, nil
	})

	retrying := stream.RetryFlowWithBackoff(
		stream.RestartSettings{
			MinBackoff:  time.Millisecond,
			MaxBackoff:  5 * time.Millisecond,
			MaxRestarts: 5,
		},
		flow,
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3}), retrying),
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
	for i, v := range result {
		if v != want[i] {
			t.Errorf("[%d] got %d, want %d", i, v, want[i])
		}
	}
}

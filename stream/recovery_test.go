/*
 * recovery_test.go
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

// ─── Recover ──────────────────────────────────────────────────────────────

// TestRecover_EmitsElementThenCompletes verifies that when the upstream fails,
// fn is called with the error, the returned element is emitted, and the stream
// then completes normally (no error propagated to the sink).
func TestRecover_EmitsElementThenCompletes(t *testing.T) {
	sentinelErr := errors.New("source failure")

	result, err := stream.RunWith(
		stream.Via(
			stream.Failed[int](sentinelErr),
			stream.Map(func(n int) int { return n }).Recover(func(error) int { return -1 }),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected no error after recovery, got: %v", err)
	}
	if len(result) != 1 || result[0] != -1 {
		t.Fatalf("got %v, want [-1]", result)
	}
}

// TestRecover_NoErrorPassesThrough verifies that when the upstream completes
// normally, Recover is a no-op: all elements pass through unchanged.
func TestRecover_NoErrorPassesThrough(t *testing.T) {
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.Map(func(n int) int { return n * 2 }).Recover(func(error) int { return -999 }),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
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
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestRecover_MidStreamError verifies that elements emitted before the error
// all arrive, followed by the single recovery element, with no error.
func TestRecover_MidStreamError(t *testing.T) {
	sentinelErr := errors.New("mid-stream failure")
	count := 0

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3, 4, 5}),
			stream.MapE(func(n int) (int, error) {
				count++
				if count == 3 {
					return 0, sentinelErr
				}
				return n * 10, nil
			}).Recover(func(error) int { return -1 }),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected recovery, got error: %v", err)
	}
	// Elements 1 and 2 succeed (10, 20), element 3 fails → recovery (-1).
	want := []int{10, 20, -1}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestRecover_ErrorInspection verifies that fn receives the actual error so
// that the recovery element can be tailored to the error type.
func TestRecover_ErrorInspection(t *testing.T) {
	var recovered error
	sentinelErr := errors.New("specific error")

	_, err := stream.RunWith(
		stream.Via(
			stream.Failed[int](sentinelErr),
			stream.Map(func(n int) int { return n }).Recover(func(e error) int {
				recovered = e
				return 0
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !errors.Is(recovered, sentinelErr) {
		t.Fatalf("fn received %v, want sentinelErr", recovered)
	}
}

// ─── RecoverWith ──────────────────────────────────────────────────────────

// TestRecoverWith_FailoverToBackup verifies that a failing stream is
// transparently continued by the backup source returned by fn.
func TestRecoverWith_FailoverToBackup(t *testing.T) {
	sentinelErr := errors.New("primary failure")
	backup := stream.FromSlice([]int{100, 200, 300})

	result, err := stream.RunWith(
		stream.Via(
			stream.Failed[int](sentinelErr),
			stream.Map(func(n int) int { return n }).RecoverWith(func(error) stream.Source[int, stream.NotUsed] {
				return backup
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected no error after failover, got: %v", err)
	}
	want := []int{100, 200, 300}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestRecoverWith_NoErrorPassesThrough verifies that RecoverWith is a no-op
// when the upstream completes normally.
func TestRecoverWith_NoErrorPassesThrough(t *testing.T) {
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.Map(func(n int) int { return n }).RecoverWith(func(error) stream.Source[int, stream.NotUsed] {
				return stream.FromSlice([]int{-1})
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestRecoverWith_MidStreamFailover verifies that elements before the error
// arrive, then the backup source delivers its elements to completion.
func TestRecoverWith_MidStreamFailover(t *testing.T) {
	sentinelErr := errors.New("mid-stream failure")
	count := 0

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3, 4, 5}),
			stream.MapE(func(n int) (int, error) {
				count++
				if count == 3 {
					return 0, sentinelErr
				}
				return n * 10, nil
			}).RecoverWith(func(error) stream.Source[int, stream.NotUsed] {
				return stream.FromSlice([]int{-1, -2})
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("expected no error after failover, got: %v", err)
	}
	// Elements 1,2 succeed (10,20); element 3 fails → backup [-1,-2].
	want := []int{10, 20, -1, -2}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestRecoverWith_EmptyBackup verifies that a backup source that is immediately
// empty causes the stream to complete without error and without extra elements.
func TestRecoverWith_EmptyBackup(t *testing.T) {
	sentinelErr := errors.New("primary failure")

	result, err := stream.RunWith(
		stream.Via(
			stream.Failed[int](sentinelErr),
			stream.Map(func(n int) int { return n }).RecoverWith(func(error) stream.Source[int, stream.NotUsed] {
				return stream.FromSlice([]int{}) // empty backup
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

// ─── Concat ───────────────────────────────────────────────────────────────

// TestConcat_SequentialOrder verifies that all elements from s1 arrive before
// any element from s2.
func TestConcat_SequentialOrder(t *testing.T) {
	result, err := stream.RunWith(
		stream.Concat(
			stream.FromSlice([]int{1, 2, 3}),
			stream.FromSlice([]int{4, 5, 6}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3, 4, 5, 6}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestConcat_EmptyFirst verifies that an empty s1 causes s2 to be the sole
// source.
func TestConcat_EmptyFirst(t *testing.T) {
	result, err := stream.RunWith(
		stream.Concat(
			stream.FromSlice([]int{}),
			stream.FromSlice([]int{7, 8, 9}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 || result[0] != 7 {
		t.Fatalf("got %v, want [7 8 9]", result)
	}
}

// TestConcat_FirstErrorSkipsSecond verifies that if s1 fails, s2 is never
// started and the error propagates.
func TestConcat_FirstErrorSkipsSecond(t *testing.T) {
	sentinelErr := errors.New("first source failure")
	secondStarted := false

	_, err := stream.RunWith(
		stream.Concat(
			stream.Failed[int](sentinelErr),
			stream.FromIteratorFunc(func() (int, bool, error) {
				secondStarted = true
				return 0, false, nil
			}),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
	if secondStarted {
		t.Fatal("second source was started despite first failing")
	}
}

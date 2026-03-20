/*
 * supervision_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/stream"
)

// ─── shared error sentinel ────────────────────────────────────────────────

var errSkip = errors.New("supervision: skip this element")

// skipOnThree doubles n but returns errSkip when n == 3.
func skipOnThree(n int) (int, error) {
	if n == 3 {
		return 0, errSkip
	}
	return n * 2, nil
}

// ─── MapE + Stop (default) ────────────────────────────────────────────────

// TestMapE_StopOnError verifies that the default behaviour (Stop) causes the
// stream to fail immediately with the function's error.
func TestMapE_StopOnError(t *testing.T) {
	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3, 4, 5}),
			stream.MapE(skipOnThree),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errSkip) {
		t.Fatalf("expected errSkip, got %v", err)
	}
}

// TestMapE_DefaultDecider_IsStop verifies that explicitly attaching
// DefaultDecider produces the same Stop behaviour as no strategy.
func TestMapE_DefaultDecider_IsStop(t *testing.T) {
	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.MapE(skipOnThree).WithSupervisionStrategy(stream.DefaultDecider),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if !errors.Is(err, errSkip) {
		t.Fatalf("expected errSkip, got %v", err)
	}
}

// ─── MapE + Resume ────────────────────────────────────────────────────────

// TestMapE_ResumeSkipsFailingElement verifies that with Resume, the element
// that caused the error is silently dropped and the remaining elements are
// processed correctly.
func TestMapE_ResumeSkipsFailingElement(t *testing.T) {
	flow := stream.MapE(skipOnThree).WithSupervisionStrategy(stream.ResumeDecider)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Element 3 is skipped; the rest are doubled.
	want := []int{2, 4, 8, 10}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestMapE_ResumeAllFailing verifies that a Resume strategy on a flow whose
// function always fails produces an empty result without an error.
func TestMapE_ResumeAllFailing(t *testing.T) {
	alwaysFail := func(n int) (int, error) { return 0, errSkip }

	result, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{1, 2, 3}),
			stream.MapE(alwaysFail).WithSupervisionStrategy(stream.ResumeDecider),
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

// ─── MapE + Restart ───────────────────────────────────────────────────────

// TestMapE_RestartContinues verifies that Restart on a stateless MapE stage
// continues processing after the error (same observable behaviour as Resume).
func TestMapE_RestartContinues(t *testing.T) {
	flow := stream.MapE(skipOnThree).WithSupervisionStrategy(stream.RestartDecider)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}), flow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []int{2, 4, 8, 10} // element 3 skipped, same as Resume
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// ─── Decider receives the original error ─────────────────────────────────

// TestMapE_DeciderSeesOriginalError verifies that the Decider receives the
// exact error returned by the function, not a wrapped variant.
func TestMapE_DeciderSeesOriginalError(t *testing.T) {
	var capturedErr error
	testDecider := func(err error) stream.Directive {
		capturedErr = err
		return stream.Resume
	}

	_, err := stream.RunWith(
		stream.Via(
			stream.FromSlice([]int{3}),
			stream.MapE(skipOnThree).WithSupervisionStrategy(testDecider),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	if !errors.Is(capturedErr, errSkip) {
		t.Fatalf("decider saw %v, want errSkip", capturedErr)
	}
}

// ─── Ask flow + supervision ───────────────────────────────────────────────

// TestAsk_Flow_ResumeOnTimeout verifies that an Ask flow with a Resume decider
// skips elements whose actor replies time out instead of failing the stream.
func TestAsk_Flow_ResumeOnTimeout(t *testing.T) {
	sys := newTestSystem(t)

	type SlowReq struct{ ReplyTo typed.TypedActorRef[int] }
	slowBehavior := typed.Behavior[SlowReq](func(_ typed.TypedContext[SlowReq], _ SlowReq) typed.Behavior[SlowReq] {
		return typed.Same[SlowReq]() // never replies
	})
	slowRef, stop := spawnTyped(t, sys, slowBehavior, "slow-supervised")
	defer stop()

	timeoutDecider := func(err error) stream.Directive {
		if errors.Is(err, context.DeadlineExceeded) {
			return stream.Resume
		}
		return stream.Stop
	}

	askFlow := stream.Ask(
		slowRef,
		20*time.Millisecond,
		func(_ int, replyTo typed.TypedActorRef[int]) SlowReq {
			return SlowReq{ReplyTo: replyTo}
		},
	).WithSupervisionStrategy(timeoutDecider)

	// All three elements will time out; with Resume they are all skipped.
	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3}), askFlow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result (all timed out), got %v", result)
	}
}

// TestAsk_Flow_StopOnTimeout is the complementary case: without supervision,
// the first timeout must fail the stream (identical to TestAsk_Flow_Timeout
// but placed here for symmetry).
func TestAsk_Flow_StopOnTimeout(t *testing.T) {
	sys := newTestSystem(t)

	type SlowReq2 struct{ ReplyTo typed.TypedActorRef[int] }
	slowBehavior2 := typed.Behavior[SlowReq2](func(_ typed.TypedContext[SlowReq2], _ SlowReq2) typed.Behavior[SlowReq2] {
		return typed.Same[SlowReq2]()
	})
	slowRef2, stop2 := spawnTyped(t, sys, slowBehavior2, "slow-stop")
	defer stop2()

	askFlow := stream.Ask(
		slowRef2,
		20*time.Millisecond,
		func(_ int, replyTo typed.TypedActorRef[int]) SlowReq2 {
			return SlowReq2{ReplyTo: replyTo}
		},
	) // no WithSupervisionStrategy → DefaultDecider (Stop)

	_, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1}), askFlow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

// ─── Source.WithSupervisionStrategy ──────────────────────────────────────

// TestSource_ResumeOnError verifies that supervision can also be applied
// directly to a Source (useful for sources backed by fallible iterators).
func TestSource_ResumeOnError(t *testing.T) {
	i := 0
	src := stream.FromIteratorFunc(func() (int, bool, error) {
		i++
		if i == 3 {
			return 0, false, errSkip // fail on the third pull
		}
		if i > 5 {
			return 0, false, nil // completed
		}
		return i, true, nil
	}).WithSupervisionStrategy(stream.ResumeDecider)

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Elements 1, 2 emitted before error; after Resume: 4, 5 (i increments each pull).
	if len(result) == 0 {
		t.Fatal("expected non-empty result after Resume")
	}
	for _, v := range result {
		if v == 0 {
			t.Fatalf("zero value leaked into result: %v", result)
		}
	}
}

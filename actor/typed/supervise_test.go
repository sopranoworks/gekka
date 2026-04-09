/*
 * supervise_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"
	"time"
)

func TestSupervise_RestartOnPanic(t *testing.T) {
	panicOnce := true
	var received []string

	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		if msg == "panic" && panicOnce {
			panicOnce = false
			panic("test panic")
		}
		received = append(received, msg)
		return Same[string]()
	}

	supervised := Supervise(behavior).OnFailure(StrategyRestart)

	// Simulate calling the behavior directly
	ctx := &typedContext[string]{}
	// First call panics, gets restarted (behavior is reset to original).
	// A nil return means "Same" — the supervised wrapper stays active.
	next := supervised(ctx, "panic")
	// Second call should work (after restart, behavior is reset)
	if next != nil && !isStopped(next) {
		next(ctx, "hello")
	} else {
		supervised(ctx, "hello")
	}

	if len(received) != 1 || received[0] != "hello" {
		t.Errorf("got %v, want [hello]", received)
	}
}

func TestSupervise_StopOnPanic(t *testing.T) {
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		panic("test panic")
	}

	supervised := Supervise(behavior).OnFailure(StrategyStop)
	ctx := &typedContext[string]{}
	next := supervised(ctx, "trigger")
	if !isStopped(next) {
		t.Error("expected Stopped after panic with StrategyStop")
	}
}

func TestSupervise_ResumeOnPanic(t *testing.T) {
	callCount := 0
	var received []string

	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		callCount++
		if callCount == 1 {
			panic("first call panics")
		}
		received = append(received, msg)
		return Same[string]()
	}

	supervised := Supervise(behavior).OnFailure(StrategyResume)
	ctx := &typedContext[string]{}

	// First call panics, resumes (keeps current behavior)
	next := supervised(ctx, "panic")
	if isStopped(next) {
		t.Fatal("should not be stopped with StrategyResume")
	}

	// Second call should work with the SAME behavior (not reset)
	if next != nil {
		next(ctx, "hello")
	} else {
		supervised(ctx, "hello")
	}

	if len(received) != 1 || received[0] != "hello" {
		t.Errorf("got %v, want [hello]", received)
	}
}

func TestSupervise_BackoffDelaysRestart(t *testing.T) {
	panicCount := 0
	var received []string

	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		panicCount++
		if panicCount <= 1 {
			panic("failing")
		}
		received = append(received, msg)
		return Same[string]()
	}

	opts := BackoffOptions{
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
	}
	supervised := Supervise(behavior).OnFailureWithBackoff(opts)
	ctx := &typedContext[string]{}

	start := time.Now()
	// First call panics, backoff delay applies
	next := supervised(ctx, "msg1")
	elapsed := time.Since(start)

	if elapsed < 5*time.Millisecond {
		t.Errorf("expected backoff delay, elapsed %v", elapsed)
	}

	// Second call should succeed (after reset)
	if next != nil {
		next(ctx, "msg2")
	} else {
		supervised(ctx, "msg2")
	}

	if len(received) != 1 || received[0] != "msg2" {
		t.Errorf("got %v, want [msg2]", received)
	}
}

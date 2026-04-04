/*
 * interceptor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// mockTypedContext satisfies TypedContext[string] for tests that need a ctx.
// It embeds the minimal interface requirements.
type mockTypedContext struct {
	log    *slog.Logger
	timers TimerScheduler[string]
}

func (m *mockTypedContext) Self() TypedActorRef[string]                              { return TypedActorRef[string]{} }
func (m *mockTypedContext) System() actor.ActorContext                               { return nil }
func (m *mockTypedContext) Log() *slog.Logger                                        { return m.log }
func (m *mockTypedContext) Watch(target actor.Ref)                                   {}
func (m *mockTypedContext) Unwatch(target actor.Ref)                                 {}
func (m *mockTypedContext) Stop(target actor.Ref)                                    {}
func (m *mockTypedContext) Passivate()                                               {}
func (m *mockTypedContext) Timers() TimerScheduler[string]                           { return m.timers }
func (m *mockTypedContext) Stash() StashBuffer[string]                               { return nil }
func (m *mockTypedContext) Sender() actor.Ref                                        { return nil }
func (m *mockTypedContext) Ask(_ actor.Ref, _ func(actor.Ref) any, _ func(any, error) string) {}
func (m *mockTypedContext) Spawn(_ any, _ string) (actor.Ref, error)                { return nil, nil }
func (m *mockTypedContext) SpawnAnonymous(_ any) (actor.Ref, error)                 { return nil, nil }
func (m *mockTypedContext) SystemActorOf(_ any, _ string) (actor.Ref, error)        { return nil, nil }

func newMockCtx() *mockTypedContext {
	return &mockTypedContext{log: slog.Default()}
}

// countingInterceptor counts AroundReceive calls.
type countingInterceptor[T any] struct {
	count atomic.Int64
}

func (c *countingInterceptor[T]) AroundReceive(ctx TypedContext[T], msg T, target Behavior[T]) Behavior[T] {
	c.count.Add(1)
	return target(ctx, msg)
}

// TestIntercept_CountsMessages verifies that the interceptor is called once per message.
func TestIntercept_CountsMessages(t *testing.T) {
	var received []string
	interceptor := &countingInterceptor[string]{}

	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, msg)
		return nil // Same
	}

	behavior := Intercept[string](interceptor, inner)
	ctx := newMockCtx()

	msgs := []string{"a", "b", "c"}
	for _, m := range msgs {
		behavior = callBehavior(behavior, ctx, m)
	}

	if got := interceptor.count.Load(); got != 3 {
		t.Errorf("interceptor count = %d, want 3", got)
	}
	if len(received) != 3 {
		t.Errorf("received %d messages, want 3", len(received))
	}
}

// callBehavior invokes a behavior and returns the next behavior (or the same
// behavior when the result is nil / Same).
func callBehavior[T any](b Behavior[T], ctx TypedContext[T], msg T) Behavior[T] {
	next := b(ctx, msg)
	if next == nil {
		return b
	}
	return next
}

// TestIntercept_CanSwallowMessages verifies an interceptor that drops odd-length strings.
func TestIntercept_CanSwallowMessages(t *testing.T) {
	var received []string

	evenOnly := &evenLenInterceptor{}
	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, msg)
		return nil
	}

	behavior := Intercept[string](evenOnly, inner)
	ctx := newMockCtx()

	inputs := []string{"ab", "abc", "abcd", "x"}
	for _, m := range inputs {
		behavior = callBehavior(behavior, ctx, m)
	}

	want := []string{"ab", "abcd"}
	if len(received) != len(want) {
		t.Fatalf("received %v, want %v", received, want)
	}
	for i, w := range want {
		if received[i] != w {
			t.Errorf("received[%d] = %q, want %q", i, received[i], w)
		}
	}
}

type evenLenInterceptor struct{}

func (e *evenLenInterceptor) AroundReceive(ctx TypedContext[string], msg string, target Behavior[string]) Behavior[string] {
	if len(msg)%2 == 0 {
		return target(ctx, msg)
	}
	return nil // swallow
}

// TestLogMessages_DoesNotPanic verifies that LogMessages wraps without panicking.
func TestLogMessages_DoesNotPanic(t *testing.T) {
	var received []string
	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, msg)
		return nil
	}

	behavior := LogMessages[string](slog.LevelDebug, inner)
	ctx := newMockCtx()
	behavior = callBehavior(behavior, ctx, "hello")

	if len(received) != 1 || received[0] != "hello" {
		t.Errorf("expected [hello], got %v", received)
	}
}

// TestIntercept_BehaviorTransition ensures the interceptor is re-applied after
// the inner behavior transitions to a new behavior.
func TestIntercept_BehaviorTransition(t *testing.T) {
	interceptor := &countingInterceptor[string]{}
	var received []string

	second := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, "second:"+msg)
		return nil
	}

	first := func(_ TypedContext[string], msg string) Behavior[string] {
		if msg == "switch" {
			return second
		}
		received = append(received, "first:"+msg)
		return nil
	}

	behavior := Intercept[string](interceptor, first)
	ctx := newMockCtx()

	behavior = callBehavior(behavior, ctx, "one")    // first:one
	behavior = callBehavior(behavior, ctx, "switch") // transitions → second
	behavior = callBehavior(behavior, ctx, "two")    // second:two

	if got := interceptor.count.Load(); got != 3 {
		t.Errorf("interceptor count after transition = %d, want 3", got)
	}
	want := []string{"first:one", "second:two"}
	if len(received) != len(want) {
		t.Fatalf("received %v, want %v", received, want)
	}
	for i, w := range want {
		if received[i] != w {
			t.Errorf("received[%d] = %q, want %q", i, received[i], w)
		}
	}
}

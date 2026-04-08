/*
 * behavior_test_kit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
)

// BehaviorTestKit provides synchronous, deterministic testing for typed actor
// behaviors without requiring a full actor system.  It runs behaviors directly
// in the test goroutine, capturing side-effects for assertion.
type BehaviorTestKit[T any] struct {
	t          *testing.T
	behavior   typed.Behavior[T]
	returned   typed.Behavior[T]
	ctx        *testKitContext[T]
	effects    []Effect
	mu         sync.Mutex
}

// Effect records a side-effect observed during behavior execution.
type Effect struct {
	Kind string // "spawn", "watch", "unwatch", "stop", "schedule"
	Name string
	Ref  actor.Ref
}

// NewBehaviorTestKit creates a [BehaviorTestKit] that tests the given initial
// behavior.  Messages can be delivered via [BehaviorTestKit.Run].
func NewBehaviorTestKit[T any](t *testing.T, behavior typed.Behavior[T]) *BehaviorTestKit[T] {
	t.Helper()
	btk := &BehaviorTestKit[T]{
		t:        t,
		behavior: behavior,
	}
	btk.ctx = &testKitContext[T]{btk: btk}
	return btk
}

// Run delivers msg to the current behavior and records the returned behavior.
func (btk *BehaviorTestKit[T]) Run(msg T) {
	btk.t.Helper()
	next := btk.behavior(btk.ctx, msg)
	btk.returned = next
	if next != nil {
		btk.behavior = next
	}
}

// ReturnedBehavior returns the behavior returned by the last [Run] call.
// nil means Same (keep current behavior).
func (btk *BehaviorTestKit[T]) ReturnedBehavior() typed.Behavior[T] {
	return btk.returned
}

// IsStopped returns true if the last [Run] returned the Stopped sentinel.
func (btk *BehaviorTestKit[T]) IsStopped() bool {
	if btk.returned == nil {
		return false
	}
	// Compare with the Stopped sentinel
	return fmt.Sprintf("%p", btk.returned) == fmt.Sprintf("%p", typed.Stopped[T]())
}

// Effects returns all recorded side-effects since the last [ClearEffects].
func (btk *BehaviorTestKit[T]) Effects() []Effect {
	btk.mu.Lock()
	defer btk.mu.Unlock()
	cp := make([]Effect, len(btk.effects))
	copy(cp, btk.effects)
	return cp
}

// ClearEffects removes all recorded side-effects.
func (btk *BehaviorTestKit[T]) ClearEffects() {
	btk.mu.Lock()
	defer btk.mu.Unlock()
	btk.effects = nil
}

// ExpectEffect asserts that an effect with the given kind exists.
func (btk *BehaviorTestKit[T]) ExpectEffect(kind string) {
	btk.t.Helper()
	btk.mu.Lock()
	defer btk.mu.Unlock()
	for _, e := range btk.effects {
		if e.Kind == kind {
			return
		}
	}
	btk.t.Errorf("expected effect %q, but not found in %v", kind, btk.effects)
}

// SelfInbox returns all messages sent to Self during behavior execution.
func (btk *BehaviorTestKit[T]) SelfInbox() []T {
	btk.mu.Lock()
	defer btk.mu.Unlock()
	cp := make([]T, len(btk.ctx.selfMessages))
	copy(cp, btk.ctx.selfMessages)
	return cp
}

func (btk *BehaviorTestKit[T]) addEffect(e Effect) {
	btk.mu.Lock()
	defer btk.mu.Unlock()
	btk.effects = append(btk.effects, e)
}

// ─── testKitContext ──────────────────────────────────────────────────────

// testKitContext implements [typed.TypedContext] for behavior testing.
type testKitContext[T any] struct {
	btk          *BehaviorTestKit[T]
	selfMessages []T
}

func (c *testKitContext[T]) Self() typed.TypedActorRef[T] {
	return typed.NewTypedActorRef[T](&testKitSelf[T]{ctx: c})
}

func (c *testKitContext[T]) System() actor.ActorContext {
	return &testKitActorContext{ctx: c}
}

func (c *testKitContext[T]) Log() *slog.Logger {
	return slog.Default()
}

func (c *testKitContext[T]) Watch(target actor.Ref) {
	c.btk.addEffect(Effect{Kind: "watch", Ref: target})
}

func (c *testKitContext[T]) Unwatch(target actor.Ref) {
	c.btk.addEffect(Effect{Kind: "unwatch", Ref: target})
}

func (c *testKitContext[T]) Stop(target actor.Ref) {
	c.btk.addEffect(Effect{Kind: "stop", Ref: target})
}

func (c *testKitContext[T]) Passivate() {}

func (c *testKitContext[T]) Timers() typed.TimerScheduler[T] {
	return &noopTimerScheduler[T]{}
}

func (c *testKitContext[T]) Stash() typed.StashBuffer[T] {
	return nil
}

func (c *testKitContext[T]) Sender() actor.Ref {
	return nil
}

func (c *testKitContext[T]) Ask(target actor.Ref, msgFactory func(replyTo actor.Ref) any, transform func(res any, err error) T) {
}

func (c *testKitContext[T]) Spawn(behavior any, name string) (actor.Ref, error) {
	c.btk.addEffect(Effect{Kind: "spawn", Name: name})
	return &actor.FunctionalMockRef{PathURI: "/user/" + name}, nil
}

func (c *testKitContext[T]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	c.btk.addEffect(Effect{Kind: "spawn", Name: "anonymous"})
	return &actor.FunctionalMockRef{PathURI: "/user/anonymous"}, nil
}

func (c *testKitContext[T]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	c.btk.addEffect(Effect{Kind: "spawn", Name: name})
	return &actor.FunctionalMockRef{PathURI: "/system/" + name}, nil
}

// testKitSelf implements actor.Ref for self-messages.
type testKitSelf[T any] struct {
	ctx *testKitContext[T]
}

func (s *testKitSelf[T]) Path() string { return "/user/testkit-self" }
func (s *testKitSelf[T]) Tell(msg any, sender ...actor.Ref) {
	if m, ok := msg.(T); ok {
		s.ctx.selfMessages = append(s.ctx.selfMessages, m)
	}
}

// testKitActorContext is a minimal actor.ActorContext for the test kit.
type testKitActorContext struct {
	actor.ActorContext
	ctx any // back-reference to testKitContext (type-erased)
}

func (c *testKitActorContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return &actor.FunctionalMockRef{PathURI: "/user/" + name}, nil
}

func (c *testKitActorContext) Watch(watcher actor.Ref, target actor.Ref) {}

// noopTimerScheduler is a no-op timer scheduler for testing.
type noopTimerScheduler[T any] struct{}

func (n *noopTimerScheduler[T]) StartSingleTimer(key any, msg T, delay time.Duration) {}
func (n *noopTimerScheduler[T]) StartPeriodicTimer(key any, msg T, interval time.Duration) {}
func (n *noopTimerScheduler[T]) IsTimerActive(key any) bool { return false }
func (n *noopTimerScheduler[T]) Cancel(key any) {}
func (n *noopTimerScheduler[T]) CancelAll() {}

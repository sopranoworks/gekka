/*
 * typed_actor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"reflect"
	"testing"
	"time"
)

type typedMockContext struct {
	ActorContext
	spawnedProps Props
	spawnedName  string
	stoppedRef   Ref
}

func (m *typedMockContext) ActorOf(props Props, name string) (Ref, error) {
	m.spawnedProps = props
	m.spawnedName = name
	return &typedMockRef{path: "/user/" + name}, nil
}

func (m *typedMockContext) Stop(target Ref) {
	m.stoppedRef = target
}

type typedMockRef struct {
	Ref
	path string
}

func (r *typedMockRef) Path() string { return r.path }
func (r *typedMockRef) Tell(msg any, sender ...Ref) {}

func TestSpawnTyped(t *testing.T) {
	ctx := &typedMockContext{}
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}

	ref, err := SpawnTyped(ctx, behavior, "test")
	if err != nil {
		t.Fatalf("SpawnTyped failed: %v", err)
	}

	if ref.Path() != "/user/test" {
		t.Errorf("expected path /user/test, got %s", ref.Path())
	}

	if ctx.spawnedName != "test" {
		t.Errorf("expected name test, got %s", ctx.spawnedName)
	}

	if ctx.spawnedProps.New == nil {
		t.Error("expected Props.New to be set")
	}

	actor := ctx.spawnedProps.New()
	if _, ok := actor.(*typedActor[string]); !ok {
		t.Errorf("expected *typedActor[string], got %T", actor)
	}
}

func TestSpawnChild(t *testing.T) {
	parentCtx := &typedMockContext{}
	parentBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		if msg == "spawn" {
			childBehavior := func(ctx TypedContext[int], msg int) Behavior[int] {
				return Same[int]()
			}
			_, _ = SpawnChild(ctx, childBehavior, "child")
		}
		return Same[string]()
	}

	// Manually create the parent typed actor and its context
	parentActor := newTypedActor(parentBehavior)
	parentTypedCtx := &typedContext[string]{actor: parentActor}

	// We need to set the system of the parent actor
	parentActor.setSystem(parentCtx)

	_, err := SpawnChild(parentTypedCtx, func(ctx TypedContext[int], msg int) Behavior[int] {
		return Same[int]()
	}, "child")

	if err != nil {
		t.Fatalf("SpawnChild failed: %v", err)
	}

	if parentCtx.spawnedName != "child" {
		t.Errorf("expected name child, got %s", parentCtx.spawnedName)
	}
}

func TestBehaviorTransition(t *testing.T) {
	var count int
	var behavior2 Behavior[string]
	behavior1 := func(ctx TypedContext[string], msg string) Behavior[string] {
		count++
		return behavior2
	}
	behavior2 = func(ctx TypedContext[string], msg string) Behavior[string] {
		count += 10
		return Same[string]()
	}

	actor := newTypedActor(behavior1)
	actor.Receive("one")   // count = 1, next = behavior2
	actor.Receive("two")   // count = 11, next = Same (behavior2)
	actor.Receive("three") // count = 21

	if count != 21 {
		t.Errorf("expected count 21, got %d", count)
	}
}

func TestSetupBehavior(t *testing.T) {
	var setupCalled bool
	behavior := Setup(func(ctx TypedContext[string]) Behavior[string] {
		setupCalled = true
		return func(ctx TypedContext[string], msg string) Behavior[string] {
			return Same[string]()
		}
	})

	actor := newTypedActor(behavior)
	if setupCalled {
		t.Error("Setup called too early")
	}

	actor.Receive("start")
	if !setupCalled {
		t.Error("Setup not called after first message")
	}
}

func TestIsStopped(t *testing.T) {
	s1 := Stopped[string]()
	s2 := Stopped[string]()
	t.Logf("s1 pointer: %v", reflect.ValueOf(s1).Pointer())
	t.Logf("s2 pointer: %v", reflect.ValueOf(s2).Pointer())
	if reflect.ValueOf(s1).Pointer() != reflect.ValueOf(s2).Pointer() {
		t.Errorf("Stopped[string]() should return the same pointer")
	}
}

func TestSameSentinel(t *testing.T) {
	var count int
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		count++
		return Same[string]()
	}

	actor := newTypedActor(behavior)
	actor.Receive("one")
	actor.Receive("two")

	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

type pingMsg struct {
	replyTo TypedActorRef[string]
}

func TestAsk(t *testing.T) {
	ctx := context.Background()

	behavior := func(ctx TypedContext[pingMsg], msg pingMsg) Behavior[pingMsg] {
		msg.replyTo.Tell("pong")
		return Same[pingMsg]()
	}
	
	target := &mockTypedRef[pingMsg]{
		behavior: behavior,
	}
	target.ctx = &typedContext[pingMsg]{actor: &typedActor[pingMsg]{}}

	tref := NewTypedActorRef[pingMsg](target)

	reply, err := Ask(ctx, tref, 100*time.Millisecond, func(replyTo TypedActorRef[string]) pingMsg {
		return pingMsg{replyTo: replyTo}
	})

	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	if reply != "pong" {
		t.Errorf("expected pong, got %s", reply)
	}
}

type mockTypedRef[T any] struct {
	Ref
	behavior Behavior[T]
	ctx      TypedContext[T]
}

func (m *mockTypedRef[T]) Tell(msg any, sender ...Ref) {
	if mmsg, ok := msg.(T); ok {
		m.behavior(m.ctx, mmsg)
	}
}
func (m *mockTypedRef[T]) Path() string { return "/test/target" }

// Benchmarks

func BenchmarkUntypedReceive(b *testing.B) {
	actor := &untypedMockActor{BaseActor: NewBaseActor()}
	msg := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		actor.Receive(msg)
	}
}

func BenchmarkTypedReceive(b *testing.B) {
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}
	actor := newTypedActor(behavior)
	msg := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		actor.Receive(msg)
	}
}

type untypedMockActor struct {
	BaseActor
}

func (a *untypedMockActor) Receive(msg any) {}

func BenchmarkUntypedAsk(b *testing.B) {
	ctx := context.Background()
	target := &untypedReplyingActor{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = untypedAsk(ctx, target, "ping")
	}
}

func BenchmarkTypedAsk(b *testing.B) {
	ctx := context.Background()
	behavior := func(ctx TypedContext[pingMsg], msg pingMsg) Behavior[pingMsg] {
		msg.replyTo.Tell("pong")
		return Same[pingMsg]()
	}
	
	target := &mockTypedRef[pingMsg]{
		behavior: behavior,
	}
	target.ctx = &typedContext[pingMsg]{actor: &typedActor[pingMsg]{}}
	tref := NewTypedActorRef[pingMsg](target)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Ask(ctx, tref, 0, func(replyTo TypedActorRef[string]) pingMsg {
			return pingMsg{replyTo: replyTo}
		})
	}
}

type untypedReplyingActor struct {
	Ref
}

func (a *untypedReplyingActor) Tell(msg any, sender ...Ref) {
	if len(sender) > 0 && sender[0] != nil {
		sender[0].Tell("pong")
	}
}
func (a *untypedReplyingActor) Path() string { return "" }

func untypedAsk(ctx context.Context, target Ref, msg any) (any, error) {
	replyCh := make(chan any, 1)
	responder := &untypedAskResponder{replyCh: replyCh}
	target.Tell(msg, responder)
	select {
	case r := <-replyCh:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type untypedAskResponder struct {
	Ref
	replyCh chan any
}

func (r *untypedAskResponder) Tell(msg any, sender ...Ref) {
	r.replyCh <- msg
}
func (r *untypedAskResponder) Path() string { return "" }

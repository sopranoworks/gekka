/*
 * typed_actor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

type typedMockContext struct {
	actor.ActorContext
	spawnedProps actor.Props
	spawnedName  string
	stoppedRef   actor.Ref
}

func (m *typedMockContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	m.spawnedProps = props
	m.spawnedName = name
	return &typedMockRef{path: "/user/" + name}, nil
}

func (m *typedMockContext) Stop(target actor.Ref) {
	m.stoppedRef = target
}

type typedMockRef struct {
	actor.Ref
	path string
}

func (r *typedMockRef) Path() string                      { return r.path }
func (r *typedMockRef) Tell(msg any, sender ...actor.Ref) {}

func TestSpawn(t *testing.T) {
	ctx := &typedMockContext{}
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}

	ref, err := Spawn(ctx, behavior, "test")
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
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

	act := ctx.spawnedProps.New()
	if _, ok := act.(*typedActor[string]); !ok {
		t.Errorf("expected *typedActor[string], got %T", act)
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
	parentActor.SetSystem(parentCtx)

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

	act := newTypedActor(behavior1)
	act.Receive("one")   // count = 1, next = behavior2
	act.Receive("two")   // count = 11, next = Same (behavior2)
	act.Receive("three") // count = 21

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

	act := newTypedActor(behavior)
	if setupCalled {
		t.Error("Setup called too early")
	}

	act.Receive("start")
	if !setupCalled {
		t.Error("Setup not called after first message")
	}
}

func TestIsStopped(t *testing.T) {
	s1 := Stopped[string]()
	s2 := Stopped[string]()
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

	act := newTypedActor(behavior)
	act.Receive("one")
	act.Receive("two")

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
	actor.Ref
	behavior Behavior[T]
	ctx      TypedContext[T]
}

func (m *mockTypedRef[T]) Tell(msg any, sender ...actor.Ref) {
	if mmsg, ok := msg.(T); ok {
		m.behavior(m.ctx, mmsg)
	}
}
func (m *mockTypedRef[T]) Path() string { return "/test/target" }

// Benchmarks

func BenchmarkUntypedReceive(b *testing.B) {
	act := &untypedMockActor{BaseActor: actor.NewBaseActor()}
	msg := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		act.Receive(msg)
	}
}

func BenchmarkTypedReceive(b *testing.B) {
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}
	act := newTypedActor(behavior)
	msg := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		act.Receive(msg)
	}
}

type untypedMockActor struct {
	actor.BaseActor
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
	actor.Ref
}

func (a *untypedReplyingActor) Tell(msg any, sender ...actor.Ref) {
	if len(sender) > 0 && sender[0] != nil {
		sender[0].Tell("pong")
	}
}
func (a *untypedReplyingActor) Path() string { return "" }

func untypedAsk(ctx context.Context, target actor.Ref, msg any) (any, error) {
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
	actor.Ref
	replyCh chan any
}

func (r *untypedAskResponder) Tell(msg any, sender ...actor.Ref) {
	r.replyCh <- msg
}
func (r *untypedAskResponder) Path() string { return "" }

type halRequest struct {
	replyTo TypedActorRef[halResponse]
}
type halResponse struct {
	answer string
}

func halBehavior() Behavior[halRequest] {
	return func(ctx TypedContext[halRequest], msg halRequest) Behavior[halRequest] {
		msg.replyTo.Tell(halResponse{answer: "I'm sorry, Dave. I'm afraid I can't do that."})
		return Same[halRequest]()
	}
}

type daveCommand struct{}
type daveInternalMsg struct {
	answer string
	err    error
}

func daveBehavior(hal TypedActorRef[halRequest], probe chan string) Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		switch m := msg.(type) {
		case daveCommand:
			ctx.Ask(hal.Untyped(), func(replyTo actor.Ref) any {
				return halRequest{replyTo: ToTyped[halResponse](replyTo)}
			}, func(res any, err error) any {
				if err != nil {
					return daveInternalMsg{err: err}
				}
				return daveInternalMsg{answer: res.(halResponse).answer}
			})
		case daveInternalMsg:
			if m.err != nil {
				probe <- "Error: " + m.err.Error()
			} else {
				probe <- m.answer
			}
		}
		return Same[any]()
	}
}

func TestContextAsk_HALandDave(t *testing.T) {
	sys := &actor.ScatterGatherTestSystem{T: t}
	probe := make(chan string, 1)

	// Spawn HAL
	hal := newTypedActor(halBehavior())
	hal.SetSystem(sys)
	halRef_mock := &actor.FunctionalMockRef{
		PathURI: "/user/hal",
		Handler: func(m any) {
			hal.Receive(m)
		},
	}
	hal.SetSelf(halRef_mock)
	actor.Start(hal)
	halRef := ToTyped[halRequest](halRef_mock)

	// Spawn Dave
	dave := newTypedActor(daveBehavior(halRef, probe))
	dave.SetSystem(sys)
	daveRef_mock := &actor.FunctionalMockRef{
		PathURI: "/user/dave",
		Handler: func(m any) {
			dave.Receive(m)
		},
	}
	dave.SetSelf(daveRef_mock)
	dave.PreStart() // Initialize timers
	actor.Start(dave)

	// Send command to Dave via mock ref to ensure goroutine safety
	daveRef_mock.Tell(daveCommand{})

	// Assert probe received HAL's answer
	select {
	case result := <-probe:
		expected := "I'm sorry, Dave. I'm afraid I can't do that."
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Dave's adapted message")
	}
}

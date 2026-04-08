/*
 * spawn_protocol_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

func TestSpawnProtocol_SpawnViaMessage(t *testing.T) {
	// Test the spawnRequestHandler interface
	greeterBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}

	var response SpawnResponse[string]
	replyRef := NewTypedActorRef[SpawnResponse[string]](&replyCapture[SpawnResponse[string]]{
		captured: &response,
	})

	req := SpawnRequest[string]{
		Behavior: greeterBehavior,
		Name:     "greeter",
		ReplyTo:  replyRef,
	}

	// Verify SpawnRequest implements spawnRequestHandler
	if _, ok := any(req).(spawnRequestHandler); !ok {
		t.Fatal("SpawnRequest should implement spawnRequestHandler")
	}

	// Test the guardian behavior with a mock context that has a working System()
	guardian := SpawnProtocolBehavior()

	mockSys := &typedMockContext{}
	ta := NewTypedActorInternal[any](guardian)
	ta.SetSelf(&typedMockRef{path: "/user/guardian"})
	ta.PreStart()

	// Set up the context to have a working System()
	ta.ctx.actor = ta
	// Override System() — the typedContext delegates to BaseActor which needs
	// an ActorContext. We'll test through the handle method directly instead.

	// Direct test of handle: SpawnRequest needs ctx.System().ActorOf()
	// We use the spawnRequestHandler interface with a guardian context
	// that has a mock system.
	guardianCtx := &typedContext[any]{
		actor: &TypedActor[any]{
			BaseActor: actor.NewBaseActor(),
		},
	}
	guardianCtx.actor.ctx = guardianCtx
	guardianCtx.actor.SetSelf(&typedMockRef{path: "/user/guardian"})

	// Set the system context to a mock
	guardianCtx.actor.BaseActor.SetSystem(mockSys)

	// Now call the guardian behavior
	next := guardian(guardianCtx, req)
	if isStopped(next) {
		t.Error("guardian should not stop after spawn request")
	}

	// Check that spawn was attempted
	if mockSys.spawnedName != "greeter" {
		t.Errorf("expected spawned name 'greeter', got '%s'", mockSys.spawnedName)
	}

	// Check response was sent
	if response.Err != nil {
		t.Errorf("unexpected error in response: %v", response.Err)
	}
	if response.Ref.Path() != "/user/greeter" {
		t.Errorf("expected ref path '/user/greeter', got '%s'", response.Ref.Path())
	}
}

func TestSpawnProtocol_UnexpectedMessage(t *testing.T) {
	guardian := SpawnProtocolBehavior()
	ctx := &typedContext[any]{
		actor: &TypedActor[any]{
			BaseActor: actor.NewBaseActor(),
		},
	}
	ctx.actor.ctx = ctx

	// Unexpected message should not crash
	next := guardian(ctx, "unexpected")
	if isStopped(next) {
		t.Error("guardian should not stop on unexpected message")
	}
}

// replyCapture is a test helper that captures Tell'd messages.
type replyCapture[T any] struct {
	captured *T
}

func (r *replyCapture[T]) Path() string                      { return "/temp/reply" }
func (r *replyCapture[T]) Tell(msg any, sender ...actor.Ref) {
	if v, ok := msg.(T); ok {
		*r.captured = v
	}
}

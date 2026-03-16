/*
 * typed_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

func TestSpawn_System(t *testing.T) {
	sys, _ := NewActorSystem("test")
	behavior := func(ctx actor.TypedContext[string], msg string) actor.Behavior[string] {
		return actor.Same[string]()
	}

	ref, err := Spawn(sys, behavior, "test")
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	if ref.Path() == "" {
		t.Error("expected non-empty path")
	}

	// Verify it's registered in the system
	localSys := sys.(*localActorSystem)
	localSys.actorsMu.RLock()
	_, found := localSys.actors["/user/test"]
	localSys.actorsMu.RUnlock()
	if !found {
		t.Error("actor not found in system")
	}
}

func TestAsk_Typed(t *testing.T) {
	sys, _ := NewActorSystem("test")

	type Ping struct {
		ReplyTo actor.TypedActorRef[string]
	}

	behavior := func(ctx actor.TypedContext[Ping], msg Ping) actor.Behavior[Ping] {
		msg.ReplyTo.Tell("pong")
		return actor.Same[Ping]()
	}

	ref, err := Spawn(sys, behavior, "pinger")
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	reply, err := Ask(ctx, ref, 0, func(replyTo actor.TypedActorRef[string]) Ping {
		return Ping{ReplyTo: replyTo}
	})

	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	if reply != "pong" {
		t.Errorf("expected pong, got %s", reply)
	}
}

func TestToTyped_ToUntyped(t *testing.T) {
	sys, _ := NewActorSystem("test")
	ref, _ := sys.ActorOf(actor.Props{New: func() actor.Actor {
		return &mockActor{BaseActor: actor.NewBaseActor()}
	}}, "untyped")

	typed := ToTyped[string](ref)
	if typed.Path() != ref.Path() {
		t.Errorf("expected path %s, got %s", ref.Path(), typed.Path())
	}

	untyped := ToUntyped(typed)
	if untyped.Path() != ref.Path() {
		t.Errorf("expected path %s, got %s", ref.Path(), untyped.Path())
	}
}

type mockActor struct {
	actor.BaseActor
}

func (a *mockActor) Receive(msg any) {}

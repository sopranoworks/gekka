/*
 * actor_system_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"strings"
	"testing"
	"time"

	"gekka/gekka/actor"
)

// simpleActor is a minimal actor used in ActorSystem tests.
type simpleActor struct {
	actor.BaseActor
}

func (a *simpleActor) Receive(_ any) {}

func newSimpleProps() Props {
	return Props{New: func() actor.Actor {
		return &simpleActor{BaseActor: actor.NewBaseActor()}
	}}
}

// ── ActorOf: basic path rule ──────────────────────────────────────────────────

func TestActorOf_PathStartsWithUser(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	ref, err := node.System.ActorOf(newSimpleProps(), "myActor")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}
	if !strings.HasPrefix(ref.Path(), "pekko://Sys@127.0.0.1:2552/user/") {
		t.Errorf("path %q does not start with expected prefix", ref.Path())
	}
	if !strings.Contains(ref.Path(), "/user/myActor") {
		t.Errorf("path %q does not contain /user/myActor", ref.Path())
	}
}

// ── ActorOf: uniqueness ───────────────────────────────────────────────────────

func TestActorOf_DuplicateName_ReturnsError(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)

	_, err := node.System.ActorOf(newSimpleProps(), "duplicate")
	if err != nil {
		t.Fatalf("first ActorOf failed unexpectedly: %v", err)
	}

	_, err = node.System.ActorOf(newSimpleProps(), "duplicate")
	if err == nil {
		t.Fatal("second ActorOf with same name should have returned an error, got nil")
	}
}

// ── ActorOf: auto-generated name ──────────────────────────────────────────────

func TestActorOf_EmptyName_AutoGenerates(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)

	ref1, err := node.System.ActorOf(newSimpleProps(), "")
	if err != nil {
		t.Fatalf("ActorOf (empty name) failed: %v", err)
	}
	ref2, err := node.System.ActorOf(newSimpleProps(), "")
	if err != nil {
		t.Fatalf("second ActorOf (empty name) failed: %v", err)
	}

	if ref1.Path() == "" {
		t.Error("auto-generated name produced an empty path")
	}
	if ref2.Path() == "" {
		t.Error("second auto-generated name produced an empty path")
	}
	if ref1.Path() == ref2.Path() {
		t.Errorf("auto-generated names must be unique, both got %q", ref1.Path())
	}
	if !strings.Contains(ref1.Path(), "/user/") {
		t.Errorf("auto-generated path %q does not contain /user/", ref1.Path())
	}
}

// ── ActorOf: invalid name ─────────────────────────────────────────────────────

func TestActorOf_NameWithSlash_ReturnsError(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)

	_, err := node.System.ActorOf(newSimpleProps(), "parent/child")
	if err == nil {
		t.Error("ActorOf with slash in name should return an error")
	}
}

// ── ActorOf: nil Props.New ────────────────────────────────────────────────────

func TestActorOf_NilNew_ReturnsError(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)

	_, err := node.System.ActorOf(Props{New: nil}, "nilTest")
	if err == nil {
		t.Error("ActorOf with nil Props.New should return an error")
	}
}

// ── ActorSystem.Context ───────────────────────────────────────────────────────

func TestActorSystem_Context_IsNotNil(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	ctx := node.System.Context()
	if ctx == nil {
		t.Fatal("ActorSystem.Context() must not be nil")
	}
}

func TestActorSystem_Context_MatchesNodeContext(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	if node.System.Context() != node.ctx {
		t.Error("ActorSystem.Context() must return the node's root context")
	}
}

func TestActorSystem_Context_CancelledOnShutdown(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	ctx := node.System.Context()

	node.cancel() // simulate node shutdown

	select {
	case <-ctx.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("context was not cancelled after node shutdown")
	}
}

// ── BaseActor.System ──────────────────────────────────────────────────────────

func TestBaseActor_System_NilBeforeSpawn(t *testing.T) {
	// An actor that has not been registered yet has nil System().
	a := &simpleActor{BaseActor: actor.NewBaseActor()}
	if a.System() != nil {
		t.Errorf("System() before spawn = %v, want nil", a.System())
	}
}

func TestBaseActor_System_SetAfterSpawnActor(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &simpleActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/sys-test", a)

	if a.System() == nil {
		t.Fatal("System() is nil after SpawnActor")
	}
}

func TestBaseActor_System_SetAfterActorOf(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	var captured actor.ActorContext
	props := Props{New: func() actor.Actor {
		a := &systemCaptureActor{BaseActor: actor.NewBaseActor()}
		return a
	}}
	ref, err := node.System.ActorOf(props, "sys-capture")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}
	_ = ref

	// Retrieve the actor from the registry to inspect its System().
	node.actorsMu.RLock()
	raw := node.actors["/user/sys-capture"]
	node.actorsMu.RUnlock()

	sc, ok := raw.(*systemCaptureActor)
	if !ok {
		t.Fatalf("expected *systemCaptureActor, got %T", raw)
	}
	captured = sc.System()
	if captured == nil {
		t.Fatal("System() is nil after ActorOf")
	}
}

// ── Actor spawns a peer via System().ActorOf ─────────────────────────────────

// spawnerActor uses its System() to create a peer actor when it receives a message.
type spawnerActor struct {
	actor.BaseActor
	peerRef actor.Ref
	done    chan struct{}
}

func (a *spawnerActor) Receive(msg any) {
	if _, ok := msg.(string); ok {
		ref, err := a.System().ActorOf(actor.Props{
			New: func() actor.Actor {
				return &simpleActor{BaseActor: actor.NewBaseActor()}
			},
		}, "spawned-by-actor")
		if err == nil {
			a.peerRef = ref
		}
		select {
		case a.done <- struct{}{}:
		default:
		}
	}
}

func TestBaseActor_System_ActorOf_SpawnsPeer(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	sp := &spawnerActor{
		BaseActor: actor.NewBaseActor(),
		done:      make(chan struct{}, 1),
	}
	ref := node.SpawnActor("/user/spawner", sp)
	ref.Tell("go")

	select {
	case <-sp.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("spawner actor did not respond within 500ms")
	}

	if sp.peerRef == nil {
		t.Fatal("spawner actor did not create a peer actor")
	}
	if !strings.Contains(sp.peerRef.Path(), "/user/spawned-by-actor") {
		t.Errorf("peer path = %q, expected to contain /user/spawned-by-actor", sp.peerRef.Path())
	}
}

// ── Actor accesses node lifecycle context via System().Context ────────────────

// contextCaptureActor records the context returned by System().Context().
type contextCaptureActor struct {
	actor.BaseActor
	capturedCtx context.Context
	done        chan struct{}
}

func (a *contextCaptureActor) Receive(_ any) {
	a.capturedCtx = a.System().Context()
	select {
	case a.done <- struct{}{}:
	default:
	}
}

func TestBaseActor_System_Context_IsNodeContext(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	ca := &contextCaptureActor{
		BaseActor: actor.NewBaseActor(),
		done:      make(chan struct{}, 1),
	}
	ref := node.SpawnActor("/user/ctx-capture", ca)
	ref.Tell("check")

	select {
	case <-ca.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("actor did not respond within 500ms")
	}

	if ca.capturedCtx == nil {
		t.Fatal("System().Context() returned nil inside Receive")
	}
	if ca.capturedCtx != node.ctx {
		t.Error("System().Context() does not match node's root context")
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// systemCaptureActor is used to inspect the injected System() after ActorOf.
type systemCaptureActor struct{ actor.BaseActor }

func (a *systemCaptureActor) Receive(_ any) {}

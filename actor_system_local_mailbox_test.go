/*
 * actor_system_local_mailbox_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/mailbox"
)

// ── Test fixtures ───────────────────────────────────────────────────────────

type ctrlMailboxActor struct {
	actor.BaseActor
	got chan any
}

func (a *ctrlMailboxActor) Receive(msg any) {
	select {
	case a.got <- msg:
	default:
	}
}

// RequiresControlAwareMessageQueueSemantics declares the actor needs
// ControlAware semantics — Pekko's RequiresMessageQueue type-class
// equivalent.
func (*ctrlMailboxActor) RequiresControlAwareMessageQueueSemantics() {}

type plainEchoActor struct {
	actor.BaseActor
	got chan any
}

func (a *plainEchoActor) Receive(msg any) {
	select {
	case a.got <- msg:
	default:
	}
}

// ── Props.WithMailbox sets MailboxName ──────────────────────────────────────

func TestProps_WithMailbox_SetsMailboxName(t *testing.T) {
	p := actor.Props{New: nil}.WithMailbox("bounded-control-aware")
	if p.MailboxName != "bounded-control-aware" {
		t.Fatalf("WithMailbox did not set MailboxName; got %q", p.MailboxName)
	}
}

// ── Phase 1 mailbox factory selection — happy path ──────────────────────────

func TestSpawnActor_WithMailbox_UnboundedControlAware(t *testing.T) {
	resetMailboxGlobals(t)
	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	got := make(chan any, 4)
	props := actor.Props{New: func() actor.Actor {
		return &plainEchoActor{BaseActor: actor.NewBaseActor(), got: got}
	}}.WithMailbox("unbounded-control-aware")

	ref, err := sys.ActorOf(props, "ctrl-explicit")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ref.Tell("hello")
	select {
	case msg := <-got:
		if msg != "hello" {
			t.Fatalf("Receive = %v, want hello", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message dispatched via Phase 1 mailbox")
	}
}

func TestSpawnActor_RequirementBindsToControlAwareDefault(t *testing.T) {
	resetMailboxGlobals(t)
	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	got := make(chan any, 4)
	props := actor.Props{New: func() actor.Actor {
		return &ctrlMailboxActor{BaseActor: actor.NewBaseActor(), got: got}
	}}

	ref, err := sys.ActorOf(props, "ctrl-required")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ref.Tell("ping")
	select {
	case msg := <-got:
		if msg != "ping" {
			t.Fatalf("Receive = %v, want ping", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("requirement-bound actor did not receive message")
	}
}

// ── Hard error on bound mismatch (Phase 1.6 contract) ───────────────────────

func TestSpawnActor_RequirementMismatchFailsStart(t *testing.T) {
	// Bind ControlAware → "unbounded" via the global config so the
	// requirement-driven resolution lands on a non-ControlAware factory.
	// The construction-time validator must hard-error and SpawnActor
	// must return a non-functional ActorRef whose Tell is a no-op.
	resetMailboxGlobals(t)
	mailbox.SetGlobalConfig(
		"",
		0,
		0,
		map[string]string{
			mailbox.RequirementControlAwareMessageQueueSemanticsID: "unbounded",
		},
	)

	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	got := make(chan any, 1)
	props := actor.Props{New: func() actor.Actor {
		return &ctrlMailboxActor{BaseActor: actor.NewBaseActor(), got: got}
	}}

	ref, _ := sys.ActorOf(props, "ctrl-mismatch")

	// The ActorRef returned from a failed construction must not deliver
	// messages — the actor goroutine never started. Tell is a no-op
	// rather than a panic.
	ref.Tell("ping")
	select {
	case <-got:
		t.Fatal("requirement mismatch should not start the actor; received message anyway")
	case <-time.After(150 * time.Millisecond):
		// Expected: nothing arrives.
	}
}

// ── Default-type HOCON override drives factory selection ───────────────────

func TestSpawnActor_DefaultTypeFromHOCONIsHonored(t *testing.T) {
	resetMailboxGlobals(t)
	mailbox.SetGlobalConfig(
		"unbounded-control-aware",
		0,
		0,
		nil,
	)

	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	got := make(chan any, 4)
	props := actor.Props{New: func() actor.Actor {
		return &plainEchoActor{BaseActor: actor.NewBaseActor(), got: got}
	}}

	ref, err := sys.ActorOf(props, "default-type")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}
	ref.Tell("hello")
	select {
	case msg := <-got:
		if msg != "hello" {
			t.Fatalf("Receive = %v, want hello", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("default-type-from-HOCON path did not deliver")
	}
}

// resetMailboxGlobals restores mailbox global state at test cleanup so
// sibling tests aren't perturbed by SetGlobalConfig calls.
func resetMailboxGlobals(t *testing.T) {
	t.Helper()
	prevType := mailbox.GlobalDefaultType()
	prevDefaults := mailbox.GlobalDefaults()
	prev := mailbox.GlobalBindings()
	bm := map[string]string{}
	if prev != nil {
		// Bindings has no exported iterator; copy via reflection-free
		// best-effort by hitting known requirement IDs. For test purposes
		// it's enough to clear the table and let SetGlobalConfig
		// repopulate.
	}
	t.Cleanup(func() {
		mailbox.SetGlobalConfig(prevType, prevDefaults.Capacity, prevDefaults.PushTimeout, bm)
	})
	mailbox.SetGlobalConfig("", 0, 0, nil)
}

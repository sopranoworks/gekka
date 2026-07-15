/*
 * actor_system_local_send_blocking_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// waitForDeadLetter polls sink until a DeadLetter matching msg/cause is
// observed or the deadline elapses.
func waitForDeadLetter(t *testing.T, sink *deadLetterSink, msg any, cause string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		for _, dl := range sink.received() {
			if dl.Message == msg && dl.Cause == cause {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("expected DeadLetter{Message:%v, Cause:%q}, got %v", msg, cause, sink.received())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestLocalActorSystem_Send_DoesNotBlockOnFullMailbox proves that the
// system-level Send path drops (and dead-letters) when the target mailbox is
// full, rather than blocking the caller indefinitely — matching ActorRef.Tell's
// drop-on-full semantics for the same condition.
func TestLocalActorSystem_Send_DoesNotBlockOnFullMailbox(t *testing.T) {
	sys, err := NewActorSystem("SendFullSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	sink := &deadLetterSink{}
	sys.EventStream().Subscribe(sink, reflect.TypeOf(actor.DeadLetter{}))

	// Mailbox size = 1. The actor picks up msg1 and blocks on the gate; msg2
	// then fills the single buffer slot, leaving the mailbox full.
	gate := make(chan struct{})
	_, err = sys.ActorOf(actor.Props{New: func() actor.Actor {
		return &gatedActor{BaseActor: actor.NewBaseActorWithSize(1), gate: gate}
	}}, "send-full")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}
	defer close(gate)

	const path = "/user/send-full"
	ctx := context.Background()

	if err := sys.Send(ctx, path, "msg1"); err != nil {
		t.Fatalf("Send msg1 (should succeed): %v", err)
	}
	time.Sleep(40 * time.Millisecond) // actor dequeues msg1 and blocks on gate
	if err := sys.Send(ctx, path, "msg2"); err != nil {
		t.Fatalf("Send msg2 (fills 1-slot buffer, should succeed): %v", err)
	}

	// The mailbox is now full. The pre-fix implementation blocks here forever.
	done := make(chan error, 1)
	go func() { done <- sys.Send(ctx, path, "overflow") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Send to a full mailbox returned nil; expected a mailbox-full error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked indefinitely on a full mailbox (regression: blocking system-level send)")
	}

	waitForDeadLetter(t, sink, "overflow", "mailbox-full")
}

// TestLocalActorSystem_SendWithSender_DoesNotBlockOnFullMailbox proves the same
// non-blocking, drop-and-dead-letter behavior for the SendWithSender ingress
// path (used by the Ask/reply routing that carries a sender path).
func TestLocalActorSystem_SendWithSender_DoesNotBlockOnFullMailbox(t *testing.T) {
	sys, err := NewActorSystem("SendWithSenderFullSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	// SendWithSender is an internal ingress method (not on the public
	// ActorSystem interface), reached here via the concrete local system.
	local := sys.(*localActorSystem)

	sink := &deadLetterSink{}
	sys.EventStream().Subscribe(sink, reflect.TypeOf(actor.DeadLetter{}))

	gate := make(chan struct{})
	_, err = sys.ActorOf(actor.Props{New: func() actor.Actor {
		return &gatedActor{BaseActor: actor.NewBaseActorWithSize(1), gate: gate}
	}}, "sws-full")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}
	defer close(gate)

	const path = "/user/sws-full"
	ctx := context.Background()

	if err := local.SendWithSender(ctx, path, "", "msg1"); err != nil {
		t.Fatalf("SendWithSender msg1 (should succeed): %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := local.SendWithSender(ctx, path, "", "msg2"); err != nil {
		t.Fatalf("SendWithSender msg2 (fills buffer, should succeed): %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- local.SendWithSender(ctx, path, "", "overflow") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("SendWithSender to a full mailbox returned nil; expected a mailbox-full error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendWithSender blocked indefinitely on a full mailbox (regression: blocking system-level send)")
	}

	// Consistent with ActorRef.Tell, the DeadLetter carries the raw payload
	// (the sender travels in DeadLetter.Sender), not the internal envelope.
	waitForDeadLetter(t, sink, "overflow", "mailbox-full")
}

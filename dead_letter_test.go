/*
 * dead_letter_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// deadLetterSink is an actor.Ref that records all DeadLetter events.
type deadLetterSink struct {
	mu   sync.Mutex
	msgs []actor.DeadLetter
}

func (d *deadLetterSink) Tell(msg any, _ ...actor.Ref) {
	if dl, ok := msg.(actor.DeadLetter); ok {
		d.mu.Lock()
		d.msgs = append(d.msgs, dl)
		d.mu.Unlock()
	}
}
func (d *deadLetterSink) Path() string { return "/test/dead-letter-sink" }

func (d *deadLetterSink) received() []actor.DeadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]actor.DeadLetter, len(d.msgs))
	copy(out, d.msgs)
	return out
}

// noopActor does nothing with received messages.
type noopActor struct{ actor.BaseActor }

func (a *noopActor) Receive(_ any) {}

// gatedActor blocks in Receive until its gate channel is closed.
type gatedActor struct {
	actor.BaseActor
	gate <-chan struct{}
}

func (a *gatedActor) Receive(_ any) { <-a.gate }

func TestDeadLetter_MailboxClosed(t *testing.T) {
	sys, err := NewActorSystem("DLClosedSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	ref, err := sys.ActorOf(actor.Props{New: func() actor.Actor {
		return &noopActor{BaseActor: actor.NewBaseActor()}
	}}, "noop-dl-closed")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	sink := &deadLetterSink{}
	sys.EventStream().Subscribe(sink, reflect.TypeOf(actor.DeadLetter{}))

	// Stop the actor to close its mailbox.
	sys.Stop(ref)
	time.Sleep(60 * time.Millisecond)

	// Send to the now-closed mailbox — should produce a DeadLetter.
	ref.Tell("lost message")
	time.Sleep(60 * time.Millisecond)

	letters := sink.received()
	if len(letters) == 0 {
		t.Fatal("expected at least one DeadLetter, got none")
	}
	dl := letters[0]
	if dl.Message != "lost message" {
		t.Errorf("DeadLetter.Message = %v, want %q", dl.Message, "lost message")
	}
	if dl.Cause != "mailbox-closed" {
		t.Errorf("DeadLetter.Cause = %q, want %q", dl.Cause, "mailbox-closed")
	}
}

func TestDeadLetter_MailboxFull(t *testing.T) {
	sys, err := NewActorSystem("DLFullSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	defer sys.Terminate()

	sink := &deadLetterSink{}
	sys.EventStream().Subscribe(sink, reflect.TypeOf(actor.DeadLetter{}))

	// gatedActor blocks in Receive until the gate is closed.
	// Mailbox size = 1: actor reads msg1 (blocks), msg2 fills the buffer,
	// msg3 overflows → DeadLetter.
	gate := make(chan struct{})
	ref, err := sys.ActorOf(actor.Props{New: func() actor.Actor {
		return &gatedActor{BaseActor: actor.NewBaseActorWithSize(1), gate: gate}
	}}, "full-dl")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ref.Tell("msg1") // actor picks this up; Receive blocks on gate
	time.Sleep(30 * time.Millisecond)
	ref.Tell("msg2")    // fills the 1-slot buffer
	ref.Tell("overflow") // mailbox full → DeadLetter
	time.Sleep(60 * time.Millisecond)
	close(gate) // unblock actor so test can clean up

	letters := sink.received()
	found := false
	for _, dl := range letters {
		if dl.Message == "overflow" && dl.Cause == "mailbox-full" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected DeadLetter{Message:overflow, Cause:mailbox-full}, got %v", letters)
	}
}

/*
 * base_actor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"
	"time"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// mockRef is a minimal Ref used in tests.
type mockRef struct{ path string }

func (r *mockRef) Tell(msg any, sender ...Ref) {}
func (r *mockRef) Path() string                { return r.path }

// captureActor records the Sender and Self seen inside Receive.
type captureActor struct {
	BaseActor
	gotSender Ref
	gotSelf   Ref
	gotMsg    any
	done      chan struct{}
}

func (a *captureActor) Receive(msg any) {
	a.gotSender = a.Sender()
	a.gotSelf = a.Self()
	a.gotMsg = msg
	select {
	case a.done <- struct{}{}:
	default:
	}
}

func newCapture() *captureActor {
	return &captureActor{
		BaseActor: NewBaseActor(),
		done:      make(chan struct{}, 1),
	}
}

func waitDone(t *testing.T, c *captureActor) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("actor did not receive message within 500ms")
	}
}

// ── NoSender ─────────────────────────────────────────────────────────────────

func TestNoSender_IsNil(t *testing.T) {
	if NoSender != nil {
		t.Errorf("NoSender must be nil, got %v", NoSender)
	}
}

// ── BaseActor.SetSelf / Self ──────────────────────────────────────────────────

func TestBaseActor_Self_Default(t *testing.T) {
	var b BaseActor
	if b.Self() != nil {
		t.Errorf("Self() before SetSelf = %v, want nil", b.Self())
	}
}

func TestBaseActor_SetSelf(t *testing.T) {
	var b BaseActor
	r := &mockRef{path: "pekko://Sys@h:1/user/self"}
	b.SetSelf(r)
	if b.Self() != r {
		t.Errorf("Self() = %v, want %v", b.Self(), r)
	}
}

// ── BaseActor.Sender — plain message (no envelope) ────────────────────────────

func TestBaseActor_Sender_PlainMessage(t *testing.T) {
	a := newCapture()
	Start(a)

	a.Mailbox() <- "hello"
	waitDone(t, a)

	if a.gotSender != nil {
		t.Errorf("Sender() for plain msg = %v, want nil", a.gotSender)
	}
	if a.gotMsg != "hello" {
		t.Errorf("Receive got %v, want %q", a.gotMsg, "hello")
	}
}

// ── BaseActor.Sender — message via Envelope with sender ───────────────────────

func TestBaseActor_Sender_WithEnvelope(t *testing.T) {
	a := newCapture()
	Start(a)

	sender := &mockRef{path: "pekko://Sys@host:1/user/sender"}
	a.Mailbox() <- Envelope{Payload: "ping", Sender: sender}
	waitDone(t, a)

	if a.gotSender != sender {
		t.Errorf("Sender() = %v, want %v", a.gotSender, sender)
	}
	if a.gotMsg != "ping" {
		t.Errorf("Receive payload = %v, want %q", a.gotMsg, "ping")
	}
}

// ── Sender is cleared between Receive calls ───────────────────────────────────

func TestBaseActor_Sender_ClearedBetweenMessages(t *testing.T) {
	// Send an Envelope (sets sender) then a plain message (no sender).
	// The second Receive should see nil Sender.
	a := newCapture()
	Start(a)

	r := &mockRef{path: "pekko://S@h:1/user/r"}
	a.Mailbox() <- Envelope{Payload: "first", Sender: r}
	waitDone(t, a)
	if a.gotSender != r {
		t.Errorf("first: Sender() = %v, want %v", a.gotSender, r)
	}

	// Second message without envelope — sender must be cleared (nil).
	a.Mailbox() <- "second"
	waitDone(t, a)
	if a.gotSender != nil {
		t.Errorf("Sender() after plain msg = %v, want nil", a.gotSender)
	}
}

// ── Envelope.Sender nil ───────────────────────────────────────────────────────

func TestBaseActor_Sender_EnvelopeNilSender(t *testing.T) {
	a := newCapture()
	Start(a)

	a.Mailbox() <- Envelope{Payload: 42, Sender: nil}
	waitDone(t, a)

	if a.gotSender != nil {
		t.Errorf("Sender() for nil-sender envelope = %v, want nil", a.gotSender)
	}
	if a.gotMsg != 42 {
		t.Errorf("Receive payload = %v, want 42", a.gotMsg)
	}
}

// ── Self() inside Receive ─────────────────────────────────────────────────────

func TestBaseActor_Self_InsideReceive(t *testing.T) {
	a := newCapture()
	self := &mockRef{path: "pekko://Sys@h:1/user/self"}
	a.SetSelf(self)
	Start(a)

	a.Mailbox() <- "check-self"
	waitDone(t, a)

	if a.gotSelf != self {
		t.Errorf("Self() inside Receive = %v, want %v", a.gotSelf, self)
	}
}

// ── Envelope unwraps nested Envelopes correctly ───────────────────────────────

func TestEnvelope_NestedPayload(t *testing.T) {
	// An Envelope whose Payload is itself an Envelope: the inner Envelope
	// reaches Receive as-is (Start only unwraps one layer).
	a := newCapture()
	Start(a)

	inner := Envelope{Payload: "inner", Sender: &mockRef{path: "pekko://S@h:1/user/inner"}}
	outer := Envelope{Payload: inner, Sender: &mockRef{path: "pekko://S@h:1/user/outer"}}

	a.Mailbox() <- outer
	waitDone(t, a)

	got, ok := a.gotMsg.(Envelope)
	if !ok {
		t.Fatalf("payload = %T, want Envelope", a.gotMsg)
	}
	if got.Payload != "inner" {
		t.Errorf("inner payload = %v, want %q", got.Payload, "inner")
	}
}

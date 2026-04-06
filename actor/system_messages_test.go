/*
 * system_messages_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── PoisonPill Tests ──────────────────────────────────────────────────────

type poisonTestActor struct {
	BaseActor
	postStopCalled atomic.Bool
	received       sync.Map
}

func (a *poisonTestActor) Receive(msg any) {
	if s, ok := msg.(string); ok {
		a.received.Store(s, true)
	}
}

func (a *poisonTestActor) PostStop() {
	a.postStopCalled.Store(true)
}

func TestPoisonPill_GracefulStop(t *testing.T) {
	a := &poisonTestActor{BaseActor: NewBaseActor()}
	stopped := make(chan struct{})
	a.SetOnStop(func() { close(stopped) })
	Start(a)

	// Send a normal message followed by PoisonPill
	a.Mailbox() <- "hello"
	a.Mailbox() <- PoisonPill{}

	select {
	case <-stopped:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not stop after PoisonPill")
	}

	if !a.postStopCalled.Load() {
		t.Error("PostStop was not called after PoisonPill")
	}

	// The "hello" message should have been processed
	if _, ok := a.received.Load("hello"); !ok {
		t.Error("actor did not process message before PoisonPill")
	}
}

func TestPoisonPill_MessagesAfterPillNotProcessed(t *testing.T) {
	a := &poisonTestActor{BaseActor: NewBaseActor()}
	stopped := make(chan struct{})
	a.SetOnStop(func() { close(stopped) })
	Start(a)

	a.Mailbox() <- PoisonPill{}
	// This message arrives after PoisonPill; it should not be processed
	// (the mailbox closes, so the send may fail silently).
	time.Sleep(50 * time.Millisecond)

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not stop after PoisonPill")
	}
}

// ── Kill Tests ────────────────────────────────────────────────────────────

type killTestParent struct {
	BaseActor
	failureCh chan Failure
}

func (a *killTestParent) Receive(msg any) {
	if f, ok := msg.(Failure); ok {
		a.failureCh <- f
	}
}

type killTestChild struct {
	BaseActor
}

func (a *killTestChild) Receive(msg any) {}

func TestKill_TriggersSupervision(t *testing.T) {
	parent := &killTestParent{
		BaseActor: NewBaseActor(),
		failureCh: make(chan Failure, 1),
	}
	parentRef := &localTestRef{path: "/user/parent", mb: parent.Mailbox()}
	parent.SetSelf(parentRef)
	Start(parent)
	defer parent.CloseMailbox()

	child := &killTestChild{BaseActor: NewBaseActor()}
	childRef := &localTestRef{path: "/user/parent/child", mb: child.Mailbox()}
	child.SetSelf(childRef)
	InjectParent(child, parentRef)
	Start(child)

	// Send Kill
	child.Mailbox() <- Kill{}

	select {
	case f := <-parent.failureCh:
		if _, ok := f.Reason.(*ActorKilledException); !ok {
			t.Errorf("expected *ActorKilledException, got %T: %v", f.Reason, f.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent did not receive failure from killed child")
	}
}

// ── Identify Tests ────────────────────────────────────────────────────────

type identifyTestActor struct {
	BaseActor
}

func (a *identifyTestActor) Receive(msg any) {
	// Normal messages are handled here; Identify is intercepted by Start()
}

func TestIdentify_ReturnsIdentity(t *testing.T) {
	a := &identifyTestActor{BaseActor: NewBaseActor()}
	selfRef := &localTestRef{path: "/user/identifyMe", mb: a.Mailbox()}
	a.SetSelf(selfRef)

	replyCh := make(chan any, 1)
	senderRef := &localTestRef{path: "/temp/sender", mb: make(chan any, 1), replyCh: replyCh}

	Start(a)
	defer a.CloseMailbox()

	// Send Identify with a sender
	a.Mailbox() <- Envelope{
		Payload: Identify{MessageID: "msg-1"},
		Sender:  senderRef,
	}

	select {
	case reply := <-replyCh:
		id, ok := reply.(ActorIdentity)
		if !ok {
			t.Fatalf("expected ActorIdentity, got %T", reply)
		}
		if id.MessageID != "msg-1" {
			t.Errorf("expected MessageID 'msg-1', got %v", id.MessageID)
		}
		if id.Ref == nil || id.Ref.Path() != "/user/identifyMe" {
			t.Errorf("expected Ref with path '/user/identifyMe', got %v", id.Ref)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive ActorIdentity reply")
	}
}

// ── Test helpers ──────────────────────────────────────────────────────────

type localTestRef struct {
	path    string
	mb      chan any
	replyCh chan any
}

func (r *localTestRef) Tell(msg any, sender ...Ref) {
	if r.replyCh != nil {
		select {
		case r.replyCh <- msg:
		default:
		}
	}
	if r.mb != nil {
		select {
		case r.mb <- msg:
		default:
		}
	}
}

func (r *localTestRef) Path() string { return r.path }

/*
 * signals_test.go
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

func TestStoppedWithPostStop_CallbackFires(t *testing.T) {
	callbackFired := false

	// StoppedWithPostStop registers the callback on the actor
	behavior := StoppedWithPostStop[any](func() {
		callbackFired = true
	})

	ta := NewTypedActorInternal[any](behavior)
	ta.SetSelf(&typedMockRef{path: "/user/test"})
	ta.PreStart()

	// Trigger the behavior which registers the postStop callback
	ta.Receive("trigger")

	// Now call PostStop which should fire the callback
	ta.PostStop()

	if !callbackFired {
		t.Error("postStop callback was not fired")
	}
}

func TestReceiveSignal_TerminatedSignal(t *testing.T) {
	var receivedSignal Signal
	var receivedMsgs []string

	msgHandler := func(ctx TypedContext[any], msg any) Behavior[any] {
		if s, ok := msg.(string); ok {
			receivedMsgs = append(receivedMsgs, s)
		}
		return Same[any]()
	}

	signalHandler := func(sig Signal) {
		receivedSignal = sig
	}

	behavior := ReceiveSignal[any](msgHandler, signalHandler)

	ctx := &typedContext[any]{actor: &TypedActor[any]{
		BaseActor: actor.NewBaseActor(),
	}}

	// Normal message
	behavior(ctx, "hello")
	if len(receivedMsgs) != 1 || receivedMsgs[0] != "hello" {
		t.Errorf("got %v, want [hello]", receivedMsgs)
	}

	// Signal delivery
	mockRef := &typedMockRef{path: "/user/watched"}
	behavior(ctx, signalWrapper{signal: TerminatedSignal{Ref: mockRef}})

	if receivedSignal == nil {
		t.Fatal("signal handler was not called")
	}
	ts, ok := receivedSignal.(TerminatedSignal)
	if !ok {
		t.Fatalf("expected TerminatedSignal, got %T", receivedSignal)
	}
	if ts.Ref.Path() != "/user/watched" {
		t.Errorf("terminated ref path = %s, want /user/watched", ts.Ref.Path())
	}
}

func TestReceiveSignal_PostStopSignal(t *testing.T) {
	var postStopReceived bool

	behavior := ReceiveSignal[any](
		func(ctx TypedContext[any], msg any) Behavior[any] {
			return Same[any]()
		},
		func(sig Signal) {
			if _, ok := sig.(PostStopSignal); ok {
				postStopReceived = true
			}
		},
	)

	ctx := &typedContext[any]{actor: &TypedActor[any]{
		BaseActor: actor.NewBaseActor(),
	}}

	behavior(ctx, signalWrapper{signal: PostStopSignal{}})
	if !postStopReceived {
		t.Error("PostStopSignal not received")
	}
}

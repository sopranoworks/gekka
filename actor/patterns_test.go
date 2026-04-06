/*
 * patterns_test.go
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

type gracefulStopActor struct {
	BaseActor
}

func (a *gracefulStopActor) Receive(msg any) {
	// PoisonPill is handled by Start(); no need to handle it here.
}

func (a *gracefulStopActor) Actor() Actor {
	return a
}

type gracefulStopRef struct {
	path  string
	mb    chan any
	actor Actor
}

func (r *gracefulStopRef) Tell(msg any, sender ...Ref) {
	select {
	case r.mb <- msg:
	default:
	}
}
func (r *gracefulStopRef) Path() string  { return r.path }
func (r *gracefulStopRef) Actor() Actor  { return r.actor }

func TestGracefulStop_Success(t *testing.T) {
	a := &gracefulStopActor{BaseActor: NewBaseActor()}
	ref := &gracefulStopRef{path: "/user/stopper", mb: a.Mailbox(), actor: a}
	a.SetSelf(ref)
	Start(a)

	err := GracefulStop(ref, 2*time.Second, PoisonPill{})
	if err != nil {
		t.Fatalf("GracefulStop returned error: %v", err)
	}
}

func TestGracefulStop_Timeout(t *testing.T) {
	a := &gracefulStopActor{BaseActor: NewBaseActor()}
	ref := &gracefulStopRef{path: "/user/slow", mb: a.Mailbox(), actor: a}
	a.SetSelf(ref)
	Start(a)
	defer a.CloseMailbox()

	// Send a message that doesn't stop the actor
	err := GracefulStop(ref, 100*time.Millisecond, "not-a-poison-pill")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestGracefulStop_NilRef(t *testing.T) {
	err := GracefulStop(nil, time.Second, PoisonPill{})
	if err == nil {
		t.Fatal("expected error for nil ref, got nil")
	}
}

/*
 * actor_system_local_test.go
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

func TestLocalActorSystem_Basic(t *testing.T) {
	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	replyCh := make(chan string, 1)
	props := actor.Props{New: func() actor.Actor {
		return &echoActorLocal{BaseActor: actor.NewBaseActor(), replyCh: replyCh}
	}}

	ref, err := sys.ActorOf(props, "echo")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ref.Tell("ping")

	select {
	case msg := <-replyCh:
		if msg != "ping" {
			t.Errorf("got %q, want %q", msg, "ping")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reply")
	}
}

func TestLocalActorSystem_Ask(t *testing.T) {
	sys, err := NewActorSystem("LocalSys")
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}

	props := actor.Props{New: func() actor.Actor {
		return &replyActorLocal{BaseActor: actor.NewBaseActor()}
	}}

	ref, err := sys.ActorOf(props, "replier")
	if err != nil {
		t.Fatalf("ActorOf: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reply, err := ref.Ask(ctx, "hello")
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}

	if reply != "world" {
		t.Errorf("got %v, want %q", reply, "world")
	}
}

type echoActorLocal struct {
	actor.BaseActor
	replyCh chan string
}

func (a *echoActorLocal) Receive(msg any) {
	if s, ok := msg.(string); ok {
		a.replyCh <- s
	}
}

type replyActorLocal struct {
	actor.BaseActor
}

func (a *replyActorLocal) Receive(msg any) {
	if msg == "hello" {
		a.Sender().Tell("world")
	}
}

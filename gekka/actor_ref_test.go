/*
 * actor_ref_test.go
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

	"gekka/gekka/actor"

	"google.golang.org/protobuf/proto"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newTestNode(t *testing.T, system, host string, port uint32) *GekkaNode {
	t.Helper()
	addr := &Address{
		Protocol: proto.String("pekko"),
		System:   proto.String(system),
		Hostname: proto.String(host),
		Port:     proto.Uint32(port),
	}
	nm := NewNodeManager(addr, 0)
	return &GekkaNode{
		nm:        nm,
		localAddr: addr,
		actors:    make(map[string]actor.Actor),
	}
}

// echoActor records the last received message.
type echoActor struct {
	actor.BaseActor
	lastMsg any
}

func (a *echoActor) Receive(msg any) { a.lastMsg = msg }

// ── ActorRef ──────────────────────────────────────────────────────────────────

func TestActorRef_Path_String(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	ref := ActorRef{fullPath: "pekko://Sys@127.0.0.1:2552/user/foo", node: node}
	if ref.Path() != ref.fullPath {
		t.Errorf("Path() = %q, want %q", ref.Path(), ref.fullPath)
	}
	if ref.String() != ref.fullPath {
		t.Errorf("String() = %q, want %q", ref.String(), ref.fullPath)
	}
}

func TestActorRef_Tell_Local(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	actor.Start(a)

	ref := ActorRef{
		fullPath: "pekko://Sys@127.0.0.1:2552/user/echo",
		node:     node,
		local:    a,
	}
	ref.Tell("hello")

	// Give the goroutine a moment to process.
	time.Sleep(20 * time.Millisecond)
	if a.lastMsg != "hello" {
		t.Errorf("actor received %v, want %q", a.lastMsg, "hello")
	}
}

func TestActorRef_Tell_LocalMailboxFull(t *testing.T) {
	// A mailbox of size 0 is always full; Tell must not block.
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActorWithSize(0)}
	actor.Start(a)

	ref := ActorRef{fullPath: "pekko://Sys@127.0.0.1:2552/user/tiny", node: node, local: a}
	done := make(chan struct{})
	go func() { ref.Tell("drop me"); close(done) }()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("Tell blocked on a full mailbox")
	}
}

// ── SpawnActor ────────────────────────────────────────────────────────────────

func TestSpawnActor_ReturnsRef(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	ref := node.SpawnActor("/user/myActor", a)

	want := "pekko://Sys@127.0.0.1:2552/user/myActor"
	if ref.Path() != want {
		t.Errorf("SpawnActor path = %q, want %q", ref.Path(), want)
	}
	if ref.local != a {
		t.Error("SpawnActor returned ref with wrong local actor")
	}
}

func TestSpawnActor_RegistersActor(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/reg", a)

	node.actorsMu.RLock()
	got, ok := node.actors["/user/reg"]
	node.actorsMu.RUnlock()

	if !ok || got != a {
		t.Error("actor not found in registry after SpawnActor")
	}
}

// ── ActorSelection.Resolve ─────────────────────────────────────────────────────

func TestActorSelection_Resolve_LocalPath(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/echo", a)

	sel := node.ActorSelection("/user/echo")
	ref, err := sel.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	wantPath := "pekko://Sys@127.0.0.1:2552/user/echo"
	if ref.Path() != wantPath {
		t.Errorf("path = %q, want %q", ref.Path(), wantPath)
	}
	if ref.local != a {
		t.Error("Resolve returned wrong local actor")
	}
}

func TestActorSelection_Resolve_LocalPath_NotFound(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	sel := node.ActorSelection("/user/ghost")
	_, err := sel.Resolve(context.Background())
	if err == nil {
		t.Error("expected error for unregistered local path, got nil")
	}
}

func TestActorSelection_Resolve_RemoteURI(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	remoteURI := "pekko://ClusterSystem@10.0.0.2:2552/user/worker"
	sel := node.ActorSelection(remoteURI)
	ref, err := sel.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve failed for remote URI: %v", err)
	}
	if ref.Path() != remoteURI {
		t.Errorf("path = %q, want %q", ref.Path(), remoteURI)
	}
	if ref.local != nil {
		t.Error("remote ActorRef should have nil local")
	}
}

func TestActorSelection_Resolve_SelfAbsoluteURI(t *testing.T) {
	// An absolute URI pointing to a local actor should resolve to a local ref.
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/echo", a)

	selfURI := "pekko://Sys@127.0.0.1:2552/user/echo"
	sel := node.ActorSelection(selfURI)
	ref, err := sel.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if ref.local != a {
		t.Error("self-URI should resolve to a local actor ref")
	}
}

func TestActorSelection_Resolve_NilContext(t *testing.T) {
	// Resolve(nil) must not panic; it should fall back to node.ctx.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	node.ctx = ctx // wire a real context so the nil fallback has somewhere to fall

	a := &echoActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/echo", a)

	ref, err := node.ActorSelection("/user/echo").Resolve(nil) //nolint:staticcheck // intentional: testing nil-ctx fallback
	if err != nil {
		t.Fatalf("Resolve(nil) failed: %v", err)
	}
	wantPath := "pekko://Sys@127.0.0.1:2552/user/echo"
	if ref.Path() != wantPath {
		t.Errorf("path = %q, want %q", ref.Path(), wantPath)
	}
	if ref.local != a {
		t.Error("Resolve(nil) returned wrong local actor")
	}
}

// ── ActorSelection.Tell ────────────────────────────────────────────────────────

func TestActorSelection_Tell_Local(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	a := &echoActor{BaseActor: actor.NewBaseActor()}
	node.SpawnActor("/user/echo", a)

	node.ActorSelection("/user/echo").Tell("ping")
	time.Sleep(20 * time.Millisecond)

	if a.lastMsg != "ping" {
		t.Errorf("actor received %v, want %q", a.lastMsg, "ping")
	}
}

// ── selfPathURI ───────────────────────────────────────────────────────────────

func TestSelfPathURI_LocalSuffix(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	got := node.selfPathURI("/user/foo")
	want := "pekko://Sys@127.0.0.1:2552/user/foo"
	if got != want {
		t.Errorf("selfPathURI = %q, want %q", got, want)
	}
}

func TestSelfPathURI_AlreadyAbsolute(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	abs := "pekko://Other@10.0.0.1:2552/user/bar"
	got := node.selfPathURI(abs)
	if got != abs {
		t.Errorf("selfPathURI modified absolute path: got %q, want %q", got, abs)
	}
}

/*
 * monitor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// monitorTestRef is a minimal actor.Ref for testing.
type monitorTestRef struct {
	path    string
	mailbox chan any
}

func (r *monitorTestRef) Tell(msg any, sender ...actor.Ref) {
	if r.mailbox != nil {
		r.mailbox <- msg
	}
}
func (r *monitorTestRef) Path() string { return r.path }

// monitorTerminated satisfies actor.TerminatedMessage for testing.
type monitorTerminated struct {
	ref actor.Ref
}

func (m monitorTerminated) TerminatedActor() actor.Ref { return m.ref }

// monitorTestSystem implements actor.ActorContext for Monitor tests.
type monitorTestSystem struct {
	actor.ActorContext
	watched map[string]bool
}

func newMonitorTestSystem() *monitorTestSystem {
	return &monitorTestSystem{watched: make(map[string]bool)}
}

func (s *monitorTestSystem) Watch(watcher actor.Ref, target actor.Ref) {
	s.watched[target.Path()] = true
}

func (s *monitorTestSystem) Stop(target actor.Ref) {}

func (s *monitorTestSystem) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return &monitorTestRef{path: "/user/" + name}, nil
}

func (s *monitorTestSystem) Spawn(behavior any, name string) (actor.Ref, error) {
	return &monitorTestRef{path: "/user/" + name}, nil
}

func (s *monitorTestSystem) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return &monitorTestRef{path: "/user/$anon"}, nil
}

func (s *monitorTestSystem) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return &monitorTestRef{path: "/system/" + name}, nil
}

// TestMonitor_TerminatedSignalSynthesizesMessage verifies that when a watched
// actor stops, the Monitor wrapper intercepts the TerminatedMessage and
// delivers the synthesized typed message to the inner behavior.
func TestMonitor_TerminatedSignalSynthesizesMessage(t *testing.T) {
	var received []string
	var terminationCount atomic.Int32

	targetRef := &monitorTestRef{path: "/user/target"}

	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, msg)
		return nil // Same
	}

	monitored := Monitor[string](targetRef, func() string {
		terminationCount.Add(1)
		return "target-stopped"
	}, inner)

	// Create a TypedActor with the monitored behavior.
	ta := NewTypedActorInternal(monitored)
	sys := newMonitorTestSystem()

	// Wire up the actor's self ref so Tell works.
	selfMailbox := make(chan any, 10)
	selfRef := &monitorTestRef{path: "/user/monitor-actor", mailbox: selfMailbox}
	ta.SetSelf(selfRef)
	ta.SetSystem(sys)
	ta.PreStart()

	// Send the first message — this triggers Monitor to register the watch
	// and install the terminated hook.
	ta.Receive("hello")

	if len(received) != 1 || received[0] != "hello" {
		t.Fatalf("expected [hello], got %v", received)
	}

	// Verify the watch was registered.
	if !sys.watched["/user/target"] {
		t.Fatal("expected Watch to be called on target")
	}

	// Verify the terminated hook was installed.
	if ta.terminatedHooks == nil || ta.terminatedHooks["/user/target"] == nil {
		t.Fatal("expected terminatedHook for /user/target to be installed")
	}

	// Simulate the watched actor stopping — the actor system delivers
	// a TerminatedMessage to the watcher's Receive.
	ta.Receive(monitorTerminated{ref: targetRef})

	// The hook fires asynchronously via Tell → selfMailbox.
	select {
	case msg := <-selfMailbox:
		if s, ok := msg.(string); !ok || s != "target-stopped" {
			t.Errorf("expected 'target-stopped', got %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for synthesized message")
	}

	if terminationCount.Load() != 1 {
		t.Errorf("expected onTerminated called once, got %d", terminationCount.Load())
	}
}

// TestMonitor_RegularMessagesPassThrough verifies that non-Terminated messages
// are forwarded to the inner behavior unchanged.
func TestMonitor_RegularMessagesPassThrough(t *testing.T) {
	var received []string
	targetRef := &monitorTestRef{path: "/user/target"}

	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		received = append(received, msg)
		return nil
	}

	monitored := Monitor[string](targetRef, func() string {
		return "stopped"
	}, inner)

	ta := NewTypedActorInternal(monitored)
	sys := newMonitorTestSystem()
	selfRef := &monitorTestRef{path: "/user/test", mailbox: make(chan any, 10)}
	ta.SetSelf(selfRef)
	ta.SetSystem(sys)
	ta.PreStart()

	ta.Receive("one")
	ta.Receive("two")
	ta.Receive("three")

	want := []string{"one", "two", "three"}
	if len(received) != len(want) {
		t.Fatalf("received %v, want %v", received, want)
	}
	for i, w := range want {
		if received[i] != w {
			t.Errorf("received[%d] = %q, want %q", i, received[i], w)
		}
	}
}

// TestMonitor_UnrelatedTerminatedIgnored verifies that TerminatedMessage from
// an unwatched actor is not intercepted.
func TestMonitor_UnrelatedTerminatedIgnored(t *testing.T) {
	targetRef := &monitorTestRef{path: "/user/target"}
	otherRef := &monitorTestRef{path: "/user/other"}
	var terminationCalled bool

	inner := func(_ TypedContext[string], msg string) Behavior[string] {
		return nil
	}

	monitored := Monitor[string](targetRef, func() string {
		terminationCalled = true
		return "stopped"
	}, inner)

	ta := NewTypedActorInternal(monitored)
	sys := newMonitorTestSystem()
	selfRef := &monitorTestRef{path: "/user/test", mailbox: make(chan any, 10)}
	ta.SetSelf(selfRef)
	ta.SetSystem(sys)
	ta.PreStart()

	// Trigger setup.
	ta.Receive("init")

	// Send a TerminatedMessage from a different actor.
	ta.Receive(monitorTerminated{ref: otherRef})

	if terminationCalled {
		t.Error("onTerminated should not be called for unrelated actor")
	}
}

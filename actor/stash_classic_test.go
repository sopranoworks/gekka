/*
 * stash_classic_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"testing"
	"time"
)

// ── Test 1: Become + UnstashAll ──────────────────────────────────────────────

// lockedActor starts in "locked" mode: stashes all string messages.
// On "unlock", it Becomes the open handler and calls UnstashAll.
type lockedActor struct {
	BaseActor
	mu       sync.Mutex
	received []string
	done     chan struct{}
	expect   int
}

func (a *lockedActor) Receive(msg any) {
	switch m := msg.(type) {
	case string:
		if m == "unlock" {
			a.Become(a.open)
			a.UnstashAll()
			return
		}
		// locked: stash everything else
		a.Stash()
	}
}

func (a *lockedActor) open(msg any) {
	if s, ok := msg.(string); ok {
		a.mu.Lock()
		a.received = append(a.received, s)
		if len(a.received) >= a.expect {
			select {
			case <-a.done:
			default:
				close(a.done)
			}
		}
		a.mu.Unlock()
	}
}

func TestClassicStash_BecomeAndUnstash(t *testing.T) {
	a := &lockedActor{
		BaseActor: NewBaseActor(),
		done:      make(chan struct{}),
		expect:    3,
	}
	Start(a)
	defer func() { a.Mailbox() <- PoisonPill{} }()

	// Send 3 messages while locked — they should all be stashed.
	a.Mailbox() <- "alpha"
	a.Mailbox() <- "beta"
	a.Mailbox() <- "gamma"

	// Small delay to ensure all 3 are processed (stashed) before unlock.
	time.Sleep(50 * time.Millisecond)

	// Unlock: triggers Become(open) + UnstashAll.
	a.Mailbox() <- "unlock"

	select {
	case <-a.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unstashed messages")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(a.received))
	}
	// FIFO order
	want := []string{"alpha", "beta", "gamma"}
	for i, w := range want {
		if a.received[i] != w {
			t.Errorf("received[%d] = %q, want %q", i, a.received[i], w)
		}
	}
}

// ── Test 2: Capacity error ───────────────────────────────────────────────────

// cappedStashActor initialises a stash with capacity 2.
type cappedStashActor struct {
	BaseActor
	errors []error
	mu     sync.Mutex
	done   chan struct{}
}

func (a *cappedStashActor) PreStart() {
	// Force stash initialisation with a small capacity.
	a.classicStash = NewStashBuffer[any](2, func(msg any) {
		a.classicStashPending = append(a.classicStashPending, msg)
	})
}

func (a *cappedStashActor) Receive(msg any) {
	switch m := msg.(type) {
	case string:
		if m == "done" {
			close(a.done)
			return
		}
	}
	err := a.Stash()
	if err != nil {
		a.mu.Lock()
		a.errors = append(a.errors, err)
		a.mu.Unlock()
	}
}

func TestClassicStash_CapacityError(t *testing.T) {
	a := &cappedStashActor{
		BaseActor: NewBaseActor(),
		done:      make(chan struct{}),
	}
	Start(a)
	defer func() { a.Mailbox() <- PoisonPill{} }()

	a.Mailbox() <- "m1"
	a.Mailbox() <- "m2"
	a.Mailbox() <- "m3" // should exceed capacity

	time.Sleep(50 * time.Millisecond)
	a.Mailbox() <- "done"

	select {
	case <-a.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.errors) != 1 {
		t.Fatalf("expected 1 stash error, got %d", len(a.errors))
	}
	if a.classicStash.Size() != 2 {
		t.Errorf("stash size = %d, want 2", a.classicStash.Size())
	}
}

// ── Test 3: Empty UnstashAll is a no-op ──────────────────────────────────────

type emptyUnstashActor struct {
	BaseActor
	done chan struct{}
}

func (a *emptyUnstashActor) Receive(msg any) {
	// Call UnstashAll on empty/uninitialised stash — must not panic.
	a.UnstashAll()
	close(a.done)
}

func TestClassicStash_EmptyUnstashIsNoop(t *testing.T) {
	a := &emptyUnstashActor{
		BaseActor: NewBaseActor(),
		done:      make(chan struct{}),
	}
	Start(a)
	defer func() { a.Mailbox() <- PoisonPill{} }()

	a.Mailbox() <- "trigger"

	select {
	case <-a.done:
		// success — no panic
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

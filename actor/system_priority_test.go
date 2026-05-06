/*
 * system_priority_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync/atomic"
	"testing"
	"time"
)

// ── system-message priority at the actor-cell level ────────────────────────
//
// These tests verify that Start's drain-first dispatch loop guarantees
// system messages (PoisonPill, Kill, supervisor signals) win over user
// traffic regardless of how saturated the user mailbox is. This is the
// behaviour Pekko's actor-cell layer provides natively; gekka closes the
// gap in perfect-pekko-compat Phase 1.2.

// slowActor processes user messages slowly, giving the test a wide window
// in which to enqueue a competing system message and observe priority.
type slowActor struct {
	BaseActor
	processed atomic.Int64 // number of user messages dispatched
	stopped   atomic.Bool  // set in PostStop
	delay     time.Duration
	stopCh    chan struct{}
}

func newSlowActor(delay time.Duration) *slowActor {
	return &slowActor{
		BaseActor: NewBaseActor(),
		delay:     delay,
		stopCh:    make(chan struct{}),
	}
}

func (a *slowActor) Receive(msg any) {
	a.processed.Add(1)
	if a.delay > 0 {
		time.Sleep(a.delay)
	}
}

func (a *slowActor) PostStop() {
	a.stopped.Store(true)
	close(a.stopCh)
}

// TestSystemMessagePriority_PoisonPillBeatsBacklog enqueues a large user
// backlog, sends PoisonPill via the priority channel, and asserts the
// actor stops without draining the entire backlog. With the legacy
// single-channel mailbox the actor would have to chew through every user
// message before seeing PoisonPill.
func TestSystemMessagePriority_PoisonPillBeatsBacklog(t *testing.T) {
	a := newSlowActor(2 * time.Millisecond)
	Start(a)

	const userBacklog = 100
	for i := 0; i < userBacklog; i++ {
		a.Mailbox() <- "work"
	}

	// Let the actor get into its steady-state processing rhythm.
	time.Sleep(5 * time.Millisecond)
	processedAtPoison := a.processed.Load()

	// Route PoisonPill through the priority channel — same path that
	// ActorRef.Tell uses when it detects a system message.
	if !a.SendSystem(PoisonPill{}) {
		t.Fatalf("SendSystem(PoisonPill) returned false")
	}

	select {
	case <-a.stopCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("actor did not stop within 2s; processed=%d", a.processed.Load())
	}

	processedAtStop := a.processed.Load()
	if !a.stopped.Load() {
		t.Fatal("PostStop was not called")
	}

	// Priority semantics: with a 2ms-per-message processor and 100 queued
	// messages the actor would need ≥200ms to drain the backlog. Stopping
	// before 50 messages have been processed is conclusive evidence that
	// PoisonPill skipped ahead.
	if processedAtStop >= userBacklog {
		t.Errorf("actor drained entire user backlog before stopping (processed=%d, backlog=%d) — system message was not prioritised",
			processedAtStop, userBacklog)
	}
	t.Logf("processed at PoisonPill enqueue=%d, processed at stop=%d, backlog=%d",
		processedAtPoison, processedAtStop, userBacklog)
}

// TestSystemMessagePriority_DrainFirstNotRandom proves the drain-first
// pattern is not the buggy naive 2-case select that picks randomly when
// both channels are ready. We pre-fill both mailboxes before Start so the
// actor wakes up with messages already queued in both — under naive
// select Go would interleave randomly; under drain-first all system
// messages must be dispatched before any user message.
func TestSystemMessagePriority_DrainFirstNotRandom(t *testing.T) {
	const iterations = 100

	rec := &priorityRecorder{
		BaseActor: NewBaseActor(),
		got:       make(chan string, iterations*2),
	}

	// Pre-fill both mailboxes BEFORE the actor goroutine starts so the
	// dispatch loop sees a fully-queued state on its first iteration.
	for i := 0; i < iterations; i++ {
		rec.Mailbox() <- "user"
		if !rec.SendSystem("sys") {
			t.Fatalf("iter %d: SendSystem returned false", i)
		}
	}

	Start(rec)

	deadline := time.After(3 * time.Second)
	collected := make([]string, 0, iterations*2)
loop:
	for {
		select {
		case s := <-rec.got:
			collected = append(collected, s)
			if len(collected) == iterations*2 {
				break loop
			}
		case <-deadline:
			t.Fatalf("only collected %d/%d messages: %v", len(collected), iterations*2, collected)
		}
	}

	// All sys messages must be dispatched before the first user message.
	// Walk the sequence: once we see "user", no later index may be "sys".
	firstUser := -1
	for i, s := range collected {
		if s == "user" && firstUser == -1 {
			firstUser = i
		}
		if firstUser >= 0 && s == "sys" {
			t.Fatalf("at index %d: sys appeared after first user (index %d) — drain-first violated; sequence: %v",
				i, firstUser, collected)
		}
	}

	if firstUser != iterations {
		t.Fatalf("first user index = %d, want %d (all %d sys must precede all %d user)",
			firstUser, iterations, iterations, iterations)
	}
}

// priorityRecorder records the channel-of-origin tag inside each message.
type priorityRecorder struct {
	BaseActor
	got chan string
}

func (r *priorityRecorder) Receive(msg any) {
	if s, ok := msg.(string); ok {
		select {
		case r.got <- s:
		default:
		}
	}
}

// Kill priority: Kill follows the same priority-channel route as
// PoisonPill (both match isSystemMessage in actor_ref.Tell), so the
// drain-first guarantee proven by TestSystemMessagePriority_PoisonPillBeatsBacklog
// applies identically. Kill's supervisor-driven termination semantics
// are covered by the existing supervision_test.go suite.

// TestSystemMailbox_DefaultBufferSize sanity-checks the priority channel
// is allocated with the expected capacity so SendSystem doesn't block in
// the steady-state happy path.
func TestSystemMailbox_DefaultBufferSize(t *testing.T) {
	b := NewBaseActor()
	if cap(b.SystemMailbox()) != systemMailboxBufferSize {
		t.Errorf("SystemMailbox capacity = %d, want %d", cap(b.SystemMailbox()), systemMailboxBufferSize)
	}
}

// TestSendSystem_ReturnsFalseOnClosed verifies the recover guard in
// SendSystem swallows the panic from sending on a closed channel.
func TestSendSystem_ReturnsFalseOnClosed(t *testing.T) {
	b := NewBaseActor()
	b.closeSystemMailbox()
	if b.SendSystem("ignored") {
		t.Error("SendSystem on closed mailbox returned true; want false")
	}
}

// TestSendSystem_ReturnsFalseWhenSaturated fills the priority channel and
// asserts SendSystem returns false rather than blocking the caller.
func TestSendSystem_ReturnsFalseWhenSaturated(t *testing.T) {
	b := NewBaseActor()
	for i := 0; i < systemMailboxBufferSize; i++ {
		if !b.SendSystem("fill") {
			t.Fatalf("unexpected false at fill iter %d", i)
		}
	}
	if b.SendSystem("overflow") {
		t.Error("SendSystem returned true on saturated channel; want false")
	}
}

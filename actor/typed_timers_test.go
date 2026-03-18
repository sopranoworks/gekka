/*
 * typed_timers_test.go
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

// captureRef is a test Ref that records delivered messages in a buffered channel.
type captureRef struct {
	ch   chan any
	path string
}

func (r *captureRef) Tell(msg any, _ ...Ref) {
	select {
	case r.ch <- msg:
	default:
	}
}
func (r *captureRef) Path() string { return r.path }

func newCaptureRef(capacity int) *captureRef {
	return &captureRef{ch: make(chan any, capacity), path: "/test/capture"}
}

// ── SingleTimer ────────────────────────────────────────────────────────────

func TestTimerScheduler_SingleTimer_Fires(t *testing.T) {
	ref := newCaptureRef(1)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	ts.StartSingleTimer("tick", "hello", 30*time.Millisecond)

	if !ts.IsTimerActive("tick") {
		t.Fatal("expected timer to be active immediately after start")
	}

	select {
	case msg := <-ref.ch:
		if msg != "hello" {
			t.Errorf("expected 'hello', got %v", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("single timer did not fire within timeout")
	}

	// Allow cleanup goroutine to deregister.
	time.Sleep(10 * time.Millisecond)
	if ts.IsTimerActive("tick") {
		t.Error("expected timer to be inactive after firing")
	}
}

func TestTimerScheduler_SingleTimer_CancelBeforeFire(t *testing.T) {
	ref := newCaptureRef(1)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	ts.StartSingleTimer("tick", "cancelled", 200*time.Millisecond)
	ts.Cancel("tick")

	if ts.IsTimerActive("tick") {
		t.Error("expected timer to be inactive after cancel")
	}

	select {
	case msg := <-ref.ch:
		t.Errorf("expected no message after cancel, got %v", msg)
	case <-time.After(400 * time.Millisecond):
		// correct — no message delivered
	}
}

func TestTimerScheduler_SingleTimer_ReplacesExisting(t *testing.T) {
	ref := newCaptureRef(4)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	// Start a long-delay timer then immediately replace it.
	ts.StartSingleTimer("k", "old", 10*time.Second)
	ts.StartSingleTimer("k", "new", 30*time.Millisecond)

	select {
	case msg := <-ref.ch:
		if msg != "new" {
			t.Errorf("expected 'new', got %v", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("replacement single timer did not fire")
	}

	// Give the "old" goroutine a chance — it should have been cancelled.
	time.Sleep(50 * time.Millisecond)
	if len(ref.ch) != 0 {
		t.Errorf("unexpected extra message in channel: %v", <-ref.ch)
	}
}

// ── FixedDelay ─────────────────────────────────────────────────────────────

func TestTimerScheduler_FixedDelay_FiresMultipleTimes(t *testing.T) {
	ref := newCaptureRef(10)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	ts.StartTimerWithFixedDelay("tick", "ping", 30*time.Millisecond)

	deadline := time.After(500 * time.Millisecond)
	got := 0
	for got < 3 {
		select {
		case <-ref.ch:
			got++
		case <-deadline:
			t.Fatalf("fixed-delay timer fired only %d/3 times", got)
		}
	}
}

func TestTimerScheduler_FixedDelay_Cancel(t *testing.T) {
	ref := newCaptureRef(10)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	ts.StartTimerWithFixedDelay("tick", "ping", 20*time.Millisecond)

	// Wait for at least one delivery.
	select {
	case <-ref.ch:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("fixed-delay timer never fired")
	}

	ts.Cancel("tick")

	// Drain anything already buffered (at most one in-flight delivery).
	time.Sleep(30 * time.Millisecond)
	for len(ref.ch) > 0 {
		<-ref.ch
	}

	// No new messages should arrive after cancel.
	time.Sleep(80 * time.Millisecond)
	if n := len(ref.ch); n > 0 {
		t.Errorf("expected no new messages after cancel, got %d", n)
	}
}

// ── IsTimerActive ──────────────────────────────────────────────────────────

func TestTimerScheduler_IsTimerActive(t *testing.T) {
	ref := newCaptureRef(2)
	ts := newTimerScheduler[string](ref)
	defer ts.cancelAll()

	if ts.IsTimerActive("k") {
		t.Error("expected inactive before start")
	}

	ts.StartSingleTimer("k", "x", 200*time.Millisecond)
	if !ts.IsTimerActive("k") {
		t.Error("expected active after start")
	}

	ts.Cancel("k")
	if ts.IsTimerActive("k") {
		t.Error("expected inactive after cancel")
	}
}

// ── cancelAll ─────────────────────────────────────────────────────────────

func TestTimerScheduler_CancelAll(t *testing.T) {
	ref := newCaptureRef(10)
	ts := newTimerScheduler[string](ref)

	ts.StartSingleTimer("a", "msg-a", 10*time.Second)
	ts.StartTimerWithFixedDelay("b", "msg-b", 10*time.Second)

	ts.cancelAll()

	if ts.IsTimerActive("a") || ts.IsTimerActive("b") {
		t.Error("expected all timers to be inactive after cancelAll")
	}

	time.Sleep(50 * time.Millisecond)
	if len(ref.ch) > 0 {
		t.Errorf("expected no messages after cancelAll, got %d", len(ref.ch))
	}
}

// ── Integration: typedActor with Timers ───────────────────────────────────

func TestTypedActor_TimerDelivery(t *testing.T) {
	received := make(chan string, 4)

	var count atomic.Int32
	behavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		received <- msg
		count.Add(1)
		return Same[string]()
	}

	a := newTypedActor(behavior)
	// Wire a self ref that feeds the actor's Receive so timer messages are processed.
	selfRef := &loopbackRef{a: a}
	a.SetSelf(selfRef)
	a.setSystem(&typedMockContext{})
	a.PreStart()
	defer a.PostStop()

	a.timers.StartSingleTimer("greet", "hello from timer", 30*time.Millisecond)

	select {
	case msg := <-received:
		if msg != "hello from timer" {
			t.Errorf("unexpected message: %v", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer-triggered message never arrived")
	}
}

// loopbackRef routes Tell back into the actor's Receive, simulating a mailbox.
type loopbackRef struct {
	a *typedActor[string]
}

func (r *loopbackRef) Tell(msg any, _ ...Ref) { r.a.Receive(msg) }
func (r *loopbackRef) Path() string           { return "/test/loopback" }

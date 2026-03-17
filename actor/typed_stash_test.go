/*
 * typed_stash_test.go
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

// ── stashBuffer unit tests ─────────────────────────────────────────────────

func TestStashBuffer_StashAndUnstashAll(t *testing.T) {
	ref := newCaptureRef(8)
	sb := newStashBuffer[string](ref, 8)

	sb.Stash("a")
	sb.Stash("b")
	sb.Stash("c")

	if sb.Size() != 3 {
		t.Fatalf("expected size 3, got %d", sb.Size())
	}

	sb.UnstashAll()

	if sb.Size() != 0 {
		t.Fatalf("expected size 0 after UnstashAll, got %d", sb.Size())
	}

	want := []string{"a", "b", "c"}
	for i, w := range want {
		select {
		case got := <-ref.ch:
			if got != w {
				t.Errorf("message[%d]: want %q, got %v", i, w, got)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("message[%d] not delivered", i)
		}
	}
}

func TestStashBuffer_Overflow(t *testing.T) {
	ref := newCaptureRef(1)
	sb := newStashBuffer[string](ref, 2)

	if !sb.Stash("a") {
		t.Fatal("first Stash should succeed")
	}
	if !sb.Stash("b") {
		t.Fatal("second Stash should succeed")
	}
	if sb.Stash("c") {
		t.Error("third Stash should fail (capacity exceeded)")
	}
	if sb.Size() != 2 {
		t.Errorf("expected size 2, got %d", sb.Size())
	}
}

func TestStashBuffer_Clear(t *testing.T) {
	ref := newCaptureRef(4)
	sb := newStashBuffer[string](ref, 4)

	sb.Stash("x")
	sb.Stash("y")
	sb.Clear()

	if sb.Size() != 0 {
		t.Errorf("expected size 0 after Clear, got %d", sb.Size())
	}

	sb.UnstashAll() // should deliver nothing
	time.Sleep(20 * time.Millisecond)
	if len(ref.ch) != 0 {
		t.Errorf("expected no messages after Clear + UnstashAll")
	}
}

func TestStashBuffer_FIFOOrder(t *testing.T) {
	ref := newCaptureRef(8)
	sb := newStashBuffer[string](ref, 8)

	msgs := []string{"first", "second", "third", "fourth"}
	for _, m := range msgs {
		sb.Stash(m)
	}
	sb.UnstashAll()

	for i, want := range msgs {
		select {
		case got := <-ref.ch:
			if got != want {
				t.Errorf("position %d: want %q, got %v", i, want, got)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("message[%d] not delivered", i)
		}
	}
}

func TestStashBuffer_UnstashAllClearsBuffer(t *testing.T) {
	ref := newCaptureRef(4)
	sb := newStashBuffer[string](ref, 4)

	sb.Stash("msg")
	sb.UnstashAll()
	sb.UnstashAll() // second call should deliver nothing

	// Drain first delivery.
	<-ref.ch

	time.Sleep(20 * time.Millisecond)
	if len(ref.ch) != 0 {
		t.Error("second UnstashAll should deliver nothing")
	}
}

// ── Integration: typedActor behavior transition with Stash ─────────────────

// TestTypedActor_Stash_BehaviorTransition verifies the canonical stash pattern:
//   - "stashing" state: incoming messages are buffered via Stash()
//   - "activate" message: UnstashAll() delivers buffered messages to the self ref
//     and the behavior transitions to "active"
//
// Note: in a real actor system the unstashed messages re-enter the mailbox and
// are processed by the new behavior.  Here we verify that UnstashAll correctly
// delivers the buffered messages in FIFO order to the self reference.
func TestTypedActor_Stash_BehaviorTransition(t *testing.T) {
	// selfRef captures messages sent by UnstashAll.
	selfRef := newCaptureRef(8)
	selfRef.path = "/test/stash-actor"

	var activeBehavior Behavior[string]
	activeBehavior = func(ctx TypedContext[string], msg string) Behavior[string] {
		return Same[string]()
	}

	stashingBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		if msg == "activate" {
			// Deliver buffered messages to self (captured by selfRef).
			ctx.Stash().UnstashAll()
			return activeBehavior
		}
		ctx.Stash().Stash(msg)
		return Same[string]()
	}

	a := newTypedActor(stashingBehavior)
	a.SetSelf(selfRef)
	a.setSystem(&typedMockContext{})
	a.PreStart()
	defer a.PostStop()

	// These arrive while in stashing behavior and are buffered.
	a.Receive("work-1")
	a.Receive("work-2")
	a.Receive("work-3")

	// Verify stash holds all three and nothing escaped to selfRef yet.
	if a.stash.Size() != 3 {
		t.Fatalf("expected stash size 3, got %d", a.stash.Size())
	}
	if len(selfRef.ch) != 0 {
		t.Fatal("expected no messages delivered to self before activate")
	}

	// Trigger transition: UnstashAll sends buffered messages to selfRef.
	a.Receive("activate")

	want := []string{"work-1", "work-2", "work-3"}
	for i, w := range want {
		select {
		case got := <-selfRef.ch:
			if got != w {
				t.Errorf("message[%d]: want %q, got %v", i, w, got)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("message[%d] not delivered after UnstashAll", i)
		}
	}

	// After activate, behavior should be activeBehavior and stash empty.
	if a.stash.Size() != 0 {
		t.Errorf("expected stash empty after UnstashAll, got %d", a.stash.Size())
	}
}

func TestTypedActor_Stash_Overflow(t *testing.T) {
	SetDefaultStashCapacity(3)
	t.Cleanup(func() { SetDefaultStashCapacity(100) })

	a := newTypedActor(func(ctx TypedContext[string], msg string) Behavior[string] {
		ctx.Stash().Stash(msg) // always stash; overflow silently discarded
		return Same[string]()
	})
	selfRef := &loopbackRef{a: a}
	a.SetSelf(selfRef)
	a.setSystem(&typedMockContext{})
	a.PreStart()
	defer a.PostStop()

	for i := range 5 {
		_ = i
		a.Receive("msg")
	}

	if a.stash.Size() != 3 {
		t.Errorf("expected stash size 3 (capacity), got %d", a.stash.Size())
	}
}

/*
 * bounded_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newBounded(t *testing.T, capacity int, pushTimeout time.Duration) Mailbox {
	t.Helper()
	mb := NewBoundedFactory().NewMailbox(Config{
		Capacity:    capacity,
		PushTimeout: pushTimeout,
	})
	t.Cleanup(mb.Close)
	return mb
}

// ── Construction guard ──────────────────────────────────────────────────────

func TestBounded_NonPositiveCapacityPanics(t *testing.T) {
	for _, cap := range []int{0, -1, -1024} {
		cap := cap
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("Capacity=%d: expected panic, got none", cap)
				}
			}()
			NewBoundedFactory().NewMailbox(Config{Capacity: cap, PushTimeout: time.Second})
		})
	}
}

// ── Order, capacity, NumberOfMessages ───────────────────────────────────────

func TestBounded_FIFOOrder(t *testing.T) {
	mb := newBounded(t, 16, time.Second)
	for i := 0; i < 16; i++ {
		if err := mb.Enqueue(i); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
	for i := 0; i < 16; i++ {
		got := mb.Dequeue()
		if got != i {
			t.Fatalf("Dequeue #%d = %v, want %d", i, got, i)
		}
	}
}

func TestBounded_NumberOfMessages(t *testing.T) {
	mb := newBounded(t, 8, time.Second)
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("empty NumberOfMessages = %d, want 0", got)
	}
	for i := 0; i < 5; i++ {
		_ = mb.Enqueue(i)
	}
	if got := mb.NumberOfMessages(); got != 5 {
		t.Fatalf("after 5 Enqueue NumberOfMessages = %d, want 5", got)
	}
	_ = mb.Dequeue()
	_ = mb.Dequeue()
	if got := mb.NumberOfMessages(); got != 3 {
		t.Fatalf("after 2 Dequeue NumberOfMessages = %d, want 3", got)
	}
}

func TestBounded_NumberOfMessagesCapsAtCapacity(t *testing.T) {
	const cap = 4
	mb := newBounded(t, cap, time.Second)
	for i := 0; i < cap; i++ {
		if err := mb.Enqueue(i); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
	if got := mb.NumberOfMessages(); got != cap {
		t.Fatalf("NumberOfMessages at full = %d, want %d", got, cap)
	}
}

// ── Validation ──────────────────────────────────────────────────────────────

func TestBounded_EnqueueNilRejected(t *testing.T) {
	mb := newBounded(t, 4, time.Second)
	err := mb.Enqueue(nil)
	if !errors.Is(err, ErrNilMessage) {
		t.Fatalf("Enqueue(nil) err = %v, want ErrNilMessage", err)
	}
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("rejected nil left %d messages, want 0", got)
	}
}

// ── Push-timeout semantics ──────────────────────────────────────────────────

func TestBounded_EnqueueFullReturnsErrMailboxFull(t *testing.T) {
	mb := newBounded(t, 2, 25*time.Millisecond)

	if err := mb.Enqueue("a"); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := mb.Enqueue("b"); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}
	// Mailbox now full — third Enqueue must time out and return ErrMailboxFull.
	start := time.Now()
	err := mb.Enqueue("c")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Enqueue at capacity err = %v, want ErrMailboxFull", err)
	}
	// Allow some slack for scheduling but the call must have actually waited.
	if elapsed < 20*time.Millisecond {
		t.Fatalf("Enqueue returned ErrMailboxFull after only %v, want ≥ ~25ms", elapsed)
	}
	// Confirm "c" was not enqueued.
	if got := mb.NumberOfMessages(); got != 2 {
		t.Fatalf("after timeout NumberOfMessages = %d, want 2", got)
	}
}

func TestBounded_EnqueueWithSpaceSucceedsImmediately(t *testing.T) {
	// Push-timeout should NOT fire when capacity is available; the
	// non-blocking fast path must short-circuit the timer entirely.
	mb := newBounded(t, 2, time.Hour) // huge timeout — only the fast path is acceptable
	start := time.Now()
	if err := mb.Enqueue("x"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("non-full Enqueue took %v, expected fast path", elapsed)
	}
}

func TestBounded_EnqueueBlocksUntilSpaceAppears(t *testing.T) {
	mb := newBounded(t, 1, 500*time.Millisecond)
	if err := mb.Enqueue("first"); err != nil {
		t.Fatalf("seed Enqueue: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue("second")
	}()

	// Give the goroutine time to enter the timer path.
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-enqDone:
		t.Fatalf("Enqueue returned before space opened: err=%v", err)
	default:
	}

	// Free space — the blocked Enqueue should succeed.
	if got := mb.Dequeue(); got != "first" {
		t.Fatalf("Dequeue = %v, want %q", got, "first")
	}

	select {
	case err := <-enqDone:
		if err != nil {
			t.Fatalf("blocked Enqueue err = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Enqueue did not unblock within 1s after Dequeue freed space")
	}
}

func TestBounded_BlockForeverWithoutTimeout(t *testing.T) {
	// PushTimeout=0 means block until space (or close).
	mb := newBounded(t, 1, 0)
	if err := mb.Enqueue("first"); err != nil {
		t.Fatalf("seed Enqueue: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue("second")
	}()

	// Should remain blocked indefinitely under no-timeout policy.
	select {
	case err := <-enqDone:
		t.Fatalf("zero-timeout Enqueue returned %v while mailbox full", err)
	case <-time.After(75 * time.Millisecond):
		// expected: still blocked
	}

	if got := mb.Dequeue(); got != "first" {
		t.Fatalf("Dequeue = %v, want %q", got, "first")
	}
	select {
	case err := <-enqDone:
		if err != nil {
			t.Fatalf("zero-timeout Enqueue final err = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("zero-timeout Enqueue never unblocked after space freed")
	}
}

// ── Close semantics ─────────────────────────────────────────────────────────

func TestBounded_EnqueueAfterCloseRejected(t *testing.T) {
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	mb.Close()
	err := mb.Enqueue("late")
	if !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue after Close err = %v, want ErrMailboxClosed", err)
	}
}

func TestBounded_CloseDrainsBufferedMessages(t *testing.T) {
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	for _, m := range []string{"a", "b", "c"} {
		if err := mb.Enqueue(m); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	mb.Close()

	for _, want := range []string{"a", "b", "c"} {
		got := mb.Dequeue()
		if got != want {
			t.Fatalf("post-close Dequeue = %v, want %q", got, want)
		}
	}
	if got := mb.Dequeue(); got != nil {
		t.Fatalf("drained-then-Dequeue = %v, want nil sentinel", got)
	}
}

func TestBounded_CloseUnblocksBlockedEnqueue(t *testing.T) {
	// A sender stuck waiting for space must observe close and return
	// ErrMailboxClosed (NOT ErrMailboxFull) once Close fires.
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 1, PushTimeout: 0})
	if err := mb.Enqueue("first"); err != nil {
		t.Fatalf("seed Enqueue: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue("second")
	}()

	// Let the sender enter the blocking select.
	time.Sleep(20 * time.Millisecond)
	mb.Close()

	select {
	case err := <-enqDone:
		if !errors.Is(err, ErrMailboxClosed) {
			t.Fatalf("blocked Enqueue post-close err = %v, want ErrMailboxClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock pending Enqueue within 1s")
	}
}

func TestBounded_CloseUnblocksWaitingDequeue(t *testing.T) {
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})

	done := make(chan any, 1)
	go func() {
		done <- mb.Dequeue()
	}()

	time.Sleep(20 * time.Millisecond)
	mb.Close()

	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("Dequeue after Close returned %v, want nil sentinel", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not unblock within 1s after Close")
	}
}

func TestBounded_CloseIsIdempotent(t *testing.T) {
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	mb.Close()
	mb.Close() // must not panic on double-close
}

// ── Misc ────────────────────────────────────────────────────────────────────

func TestBounded_DequeueBlocksUntilEnqueue(t *testing.T) {
	mb := newBounded(t, 4, time.Second)

	done := make(chan any, 1)
	go func() {
		done <- mb.Dequeue()
	}()

	select {
	case <-done:
		t.Fatal("Dequeue returned before any Enqueue")
	case <-time.After(20 * time.Millisecond):
		// expected: still blocked
	}

	if err := mb.Enqueue("delivered"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case got := <-done:
		if got != "delivered" {
			t.Fatalf("Dequeue = %v, want %q", got, "delivered")
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Enqueue")
	}
}

func TestBounded_ConcurrentProducersAndConsumers(t *testing.T) {
	// Capacity smaller than total work forces the bounded backpressure path
	// to actually engage — without push-timeout to keep the test
	// deterministic (block-forever semantics).
	mb := NewBoundedFactory().NewMailbox(Config{Capacity: 32, PushTimeout: 0})

	const producers = 4
	const consumers = 4
	const perProducer = 500
	const total = producers * perProducer

	var produced atomic.Int64
	var prodWG sync.WaitGroup
	prodWG.Add(producers)
	for p := 0; p < producers; p++ {
		go func(base int) {
			defer prodWG.Done()
			for i := 0; i < perProducer; i++ {
				if err := mb.Enqueue(base*perProducer + i); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
				produced.Add(1)
			}
		}(p)
	}

	var consumed atomic.Int64
	consumerDone := make(chan struct{})
	var consumeWG sync.WaitGroup
	consumeWG.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer consumeWG.Done()
			for {
				v := mb.Dequeue()
				if v == nil {
					return // mailbox closed and drained
				}
				consumed.Add(1)
				if consumed.Load() >= total {
					select {
					case <-consumerDone:
					default:
						close(consumerDone)
					}
				}
			}
		}()
	}

	prodWG.Wait()
	<-consumerDone
	mb.Close()
	consumeWG.Wait()

	if produced.Load() != total {
		t.Fatalf("produced = %d, want %d", produced.Load(), total)
	}
	if consumed.Load() != total {
		t.Fatalf("consumed = %d, want %d", consumed.Load(), total)
	}
}

// ── Registry ────────────────────────────────────────────────────────────────

func TestRegistry_ResolveBounded(t *testing.T) {
	cases := []string{
		"bounded",
		"org.apache.pekko.dispatch.BoundedMailbox",
		"akka.dispatch.BoundedMailbox",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			f := Resolve(id)
			if f == nil {
				t.Fatalf("Resolve(%q) = nil, want bounded factory", id)
			}
			mb := f.NewMailbox(Config{Capacity: 4, PushTimeout: time.Millisecond})
			if _, ok := mb.(*boundedMailbox); !ok {
				t.Fatalf("Resolve(%q) factory produced %T, want *boundedMailbox", id, mb)
			}
			mb.Close()
		})
	}
}

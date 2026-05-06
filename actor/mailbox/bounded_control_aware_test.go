/*
 * bounded_control_aware_test.go
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

// Test fixtures (testControlMsg/testRegularMsg) are shared with
// control_aware_test.go in the same package.

func newBoundedControlAware(t *testing.T, capacity int, pushTimeout time.Duration) Mailbox {
	t.Helper()
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{
		Capacity:    capacity,
		PushTimeout: pushTimeout,
	})
	t.Cleanup(mb.Close)
	return mb
}

// ── Construction guard ──────────────────────────────────────────────────────

func TestBoundedControlAware_NonPositiveCapacityPanics(t *testing.T) {
	for _, cap := range []int{0, -1, -1024} {
		cap := cap
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("Capacity=%d: expected panic, got none", cap)
				}
			}()
			NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: cap, PushTimeout: time.Second})
		})
	}
}

// ── Control-message priority over regular ──────────────────────────────────

func TestBoundedControlAware_ControlDequeuesAheadOfPreloadedRegulars(t *testing.T) {
	// Pre-load N regular messages, then enqueue one control message; the
	// drain-first contract requires the control message to dequeue ahead
	// of every already-queued regular message.
	const N = 256
	mb := newBoundedControlAware(t, N+8, time.Second)
	for i := 0; i < N; i++ {
		if err := mb.Enqueue(testRegularMsg{id: i}); err != nil {
			t.Fatalf("Enqueue regular(%d): %v", i, err)
		}
	}
	if err := mb.Enqueue(testControlMsg{id: 9999}); err != nil {
		t.Fatalf("Enqueue control: %v", err)
	}

	got := mb.Dequeue()
	cm, ok := got.(testControlMsg)
	if !ok {
		t.Fatalf("first Dequeue = %T (%+v), want testControlMsg", got, got)
	}
	if cm.id != 9999 {
		t.Fatalf("first Dequeue control id = %d, want 9999", cm.id)
	}
}

// ── Drain-first non-determinism guard ──────────────────────────────────────

func TestBoundedControlAware_DrainFirstIsDeterministic1000Trials(t *testing.T) {
	// Run 1000 trials of "control + regular both already queued"; the
	// dequeue must ALWAYS return control first. This guards against the
	// buggy naive 2-case select where Go picks randomly when both
	// channels are ready — without the non-blocking control-first phase,
	// this test will fail randomly within a few trials.
	const trials = 1000
	for i := 0; i < trials; i++ {
		mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
		// Enqueue regular FIRST so a naive FIFO impl would mis-prioritise;
		// only a true drain-first impl returns control first.
		if err := mb.Enqueue(testRegularMsg{id: i}); err != nil {
			mb.Close()
			t.Fatalf("trial %d Enqueue regular: %v", i, err)
		}
		if err := mb.Enqueue(testControlMsg{id: i}); err != nil {
			mb.Close()
			t.Fatalf("trial %d Enqueue control: %v", i, err)
		}
		got := mb.Dequeue()
		mb.Close()
		if _, ok := got.(testControlMsg); !ok {
			t.Fatalf("trial %d: dequeue returned %T, want testControlMsg (drain-first violated)", i, got)
		}
	}
}

// ── FIFO within each priority class ─────────────────────────────────────────

func TestBoundedControlAware_FIFOWithinControlClass(t *testing.T) {
	mb := newBoundedControlAware(t, 16, time.Second)
	for i := 0; i < 8; i++ {
		if err := mb.Enqueue(testControlMsg{id: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 8; i++ {
		got := mb.Dequeue()
		cm, ok := got.(testControlMsg)
		if !ok {
			t.Fatalf("Dequeue #%d = %T, want testControlMsg", i, got)
		}
		if cm.id != i {
			t.Fatalf("Dequeue #%d id = %d, want %d (FIFO within control class violated)", i, cm.id, i)
		}
	}
}

func TestBoundedControlAware_FIFOWithinRegularClass(t *testing.T) {
	mb := newBoundedControlAware(t, 16, time.Second)
	for i := 0; i < 8; i++ {
		if err := mb.Enqueue(testRegularMsg{id: i}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 8; i++ {
		got := mb.Dequeue()
		rm, ok := got.(testRegularMsg)
		if !ok {
			t.Fatalf("Dequeue #%d = %T, want testRegularMsg", i, got)
		}
		if rm.id != i {
			t.Fatalf("Dequeue #%d id = %d, want %d (FIFO within regular class violated)", i, rm.id, i)
		}
	}
}

func TestBoundedControlAware_DrainOrderControlBeforeRegular(t *testing.T) {
	mb := newBoundedControlAware(t, 16, time.Second)
	// Interleave: R0, C0, R1, C1, R2 — drain expected: C0, C1, R0, R1, R2.
	_ = mb.Enqueue(testRegularMsg{id: 0})
	_ = mb.Enqueue(testControlMsg{id: 0})
	_ = mb.Enqueue(testRegularMsg{id: 1})
	_ = mb.Enqueue(testControlMsg{id: 1})
	_ = mb.Enqueue(testRegularMsg{id: 2})

	wantCtrl := []int{0, 1}
	for i, want := range wantCtrl {
		got := mb.Dequeue()
		cm, ok := got.(testControlMsg)
		if !ok {
			t.Fatalf("Dequeue #%d = %T, want testControlMsg", i, got)
		}
		if cm.id != want {
			t.Fatalf("Dequeue #%d ctrl id = %d, want %d", i, cm.id, want)
		}
	}
	wantReg := []int{0, 1, 2}
	for i, want := range wantReg {
		got := mb.Dequeue()
		rm, ok := got.(testRegularMsg)
		if !ok {
			t.Fatalf("Dequeue #%d (regular) = %T, want testRegularMsg", i+2, got)
		}
		if rm.id != want {
			t.Fatalf("Dequeue regular id = %d, want %d", rm.id, want)
		}
	}
}

// ── NumberOfMessages shape covering both channels ──────────────────────────

func TestBoundedControlAware_NumberOfMessagesCoversBothChannels(t *testing.T) {
	mb := newBoundedControlAware(t, 16, time.Second)
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("empty NumberOfMessages = %d, want 0", got)
	}
	for i := 0; i < 5; i++ {
		_ = mb.Enqueue(testRegularMsg{id: i})
	}
	for i := 0; i < 3; i++ {
		_ = mb.Enqueue(testControlMsg{id: i})
	}
	if got := mb.NumberOfMessages(); got != 8 {
		t.Fatalf("NumberOfMessages after 5 regular + 3 control = %d, want 8", got)
	}
	_ = mb.Dequeue()
	if got := mb.NumberOfMessages(); got != 7 {
		t.Fatalf("NumberOfMessages after 1 dequeue = %d, want 7", got)
	}
	_ = mb.Dequeue()
	if got := mb.NumberOfMessages(); got != 6 {
		t.Fatalf("NumberOfMessages after 2 dequeues = %d, want 6", got)
	}
}

func TestBoundedControlAware_NumberOfMessagesCapsAtCombinedCapacity(t *testing.T) {
	const cap = 4
	mb := newBoundedControlAware(t, cap, time.Second)
	for i := 0; i < cap; i++ {
		if err := mb.Enqueue(testRegularMsg{id: i}); err != nil {
			t.Fatalf("Enqueue regular(%d): %v", i, err)
		}
	}
	for i := 0; i < cap; i++ {
		if err := mb.Enqueue(testControlMsg{id: i}); err != nil {
			t.Fatalf("Enqueue control(%d): %v", i, err)
		}
	}
	// Each side capped independently at `cap`; combined snapshot is 2*cap.
	if got, want := mb.NumberOfMessages(), 2*cap; got != want {
		t.Fatalf("NumberOfMessages at full = %d, want %d", got, want)
	}
}

// ── Validation ──────────────────────────────────────────────────────────────

func TestBoundedControlAware_EnqueueNilRejected(t *testing.T) {
	mb := newBoundedControlAware(t, 4, time.Second)
	err := mb.Enqueue(nil)
	if !errors.Is(err, ErrNilMessage) {
		t.Fatalf("Enqueue(nil) err = %v, want ErrNilMessage", err)
	}
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("rejected nil left %d messages, want 0", got)
	}
}

// ── Per-side push-timeout firing ───────────────────────────────────────────

func TestBoundedControlAware_RegularSidePushTimeoutFires(t *testing.T) {
	// Regular side fills to capacity; the next regular Enqueue must time
	// out and return ErrMailboxFull. Control side capacity is unrelated.
	mb := newBoundedControlAware(t, 2, 25*time.Millisecond)

	if err := mb.Enqueue(testRegularMsg{id: 0}); err != nil {
		t.Fatalf("Enqueue regular(0): %v", err)
	}
	if err := mb.Enqueue(testRegularMsg{id: 1}); err != nil {
		t.Fatalf("Enqueue regular(1): %v", err)
	}
	start := time.Now()
	err := mb.Enqueue(testRegularMsg{id: 2})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Enqueue regular at capacity err = %v, want ErrMailboxFull", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("Enqueue returned ErrMailboxFull after only %v, want ≥ ~25ms", elapsed)
	}
	// Control side must remain free even while regular side is full.
	if err := mb.Enqueue(testControlMsg{id: 100}); err != nil {
		t.Fatalf("Enqueue control while regular full: %v", err)
	}
}

func TestBoundedControlAware_ControlSidePushTimeoutFires(t *testing.T) {
	// Control side fills to capacity; the next control Enqueue must time
	// out and return ErrMailboxFull. Regular side capacity is unrelated.
	mb := newBoundedControlAware(t, 2, 25*time.Millisecond)

	if err := mb.Enqueue(testControlMsg{id: 0}); err != nil {
		t.Fatalf("Enqueue control(0): %v", err)
	}
	if err := mb.Enqueue(testControlMsg{id: 1}); err != nil {
		t.Fatalf("Enqueue control(1): %v", err)
	}
	start := time.Now()
	err := mb.Enqueue(testControlMsg{id: 2})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Enqueue control at capacity err = %v, want ErrMailboxFull", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("Enqueue returned ErrMailboxFull after only %v, want ≥ ~25ms", elapsed)
	}
	// Regular side must remain free even while control side is full.
	if err := mb.Enqueue(testRegularMsg{id: 100}); err != nil {
		t.Fatalf("Enqueue regular while control full: %v", err)
	}
}

func TestBoundedControlAware_EnqueueWithSpaceUsesFastPath(t *testing.T) {
	// Push-timeout must NOT fire when capacity is available; the
	// non-blocking fast path must short-circuit the timer.
	mb := newBoundedControlAware(t, 4, time.Hour)
	start := time.Now()
	if err := mb.Enqueue(testRegularMsg{id: 0}); err != nil {
		t.Fatalf("Enqueue regular: %v", err)
	}
	if err := mb.Enqueue(testControlMsg{id: 0}); err != nil {
		t.Fatalf("Enqueue control: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("non-full Enqueue pair took %v, expected fast path", elapsed)
	}
}

func TestBoundedControlAware_BlockForeverWithoutTimeout(t *testing.T) {
	// PushTimeout=0 means block until space (or close).
	mb := newBoundedControlAware(t, 1, 0)
	if err := mb.Enqueue(testRegularMsg{id: 0}); err != nil {
		t.Fatalf("seed Enqueue: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue(testRegularMsg{id: 1})
	}()

	select {
	case err := <-enqDone:
		t.Fatalf("zero-timeout Enqueue returned %v while regular side full", err)
	case <-time.After(75 * time.Millisecond):
		// expected: still blocked
	}

	if got := mb.Dequeue(); got != (testRegularMsg{id: 0}) {
		t.Fatalf("Dequeue = %v, want testRegularMsg{id:0}", got)
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

func TestBoundedControlAware_EnqueueAfterCloseRejected(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	mb.Close()
	if err := mb.Enqueue(testRegularMsg{id: 0}); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue regular after Close err = %v, want ErrMailboxClosed", err)
	}
	if err := mb.Enqueue(testControlMsg{id: 0}); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue control after Close err = %v, want ErrMailboxClosed", err)
	}
}

func TestBoundedControlAware_CloseDrainsBufferedPreservingPriority(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	_ = mb.Enqueue(testRegularMsg{id: 1})
	_ = mb.Enqueue(testControlMsg{id: 2})
	_ = mb.Enqueue(testRegularMsg{id: 3})
	mb.Close()

	if got := mb.Dequeue(); got != (testControlMsg{id: 2}) {
		t.Fatalf("post-close Dequeue #1 = %v, want control id=2", got)
	}
	if got := mb.Dequeue(); got != (testRegularMsg{id: 1}) {
		t.Fatalf("post-close Dequeue #2 = %v, want regular id=1", got)
	}
	if got := mb.Dequeue(); got != (testRegularMsg{id: 3}) {
		t.Fatalf("post-close Dequeue #3 = %v, want regular id=3", got)
	}
	if got := mb.Dequeue(); got != nil {
		t.Fatalf("post-drain Dequeue = %v, want nil sentinel", got)
	}
}

func TestBoundedControlAware_CloseUnblocksWaitingDequeue(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})

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

func TestBoundedControlAware_CloseUnblocksBlockedRegularEnqueue(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 1, PushTimeout: 0})
	if err := mb.Enqueue(testRegularMsg{id: 0}); err != nil {
		t.Fatalf("seed regular: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue(testRegularMsg{id: 1})
	}()
	time.Sleep(20 * time.Millisecond)
	mb.Close()

	select {
	case err := <-enqDone:
		if !errors.Is(err, ErrMailboxClosed) {
			t.Fatalf("blocked regular Enqueue post-close err = %v, want ErrMailboxClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock pending regular Enqueue within 1s")
	}
}

func TestBoundedControlAware_CloseUnblocksBlockedControlEnqueue(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 1, PushTimeout: 0})
	if err := mb.Enqueue(testControlMsg{id: 0}); err != nil {
		t.Fatalf("seed control: %v", err)
	}

	enqDone := make(chan error, 1)
	go func() {
		enqDone <- mb.Enqueue(testControlMsg{id: 1})
	}()
	time.Sleep(20 * time.Millisecond)
	mb.Close()

	select {
	case err := <-enqDone:
		if !errors.Is(err, ErrMailboxClosed) {
			t.Fatalf("blocked control Enqueue post-close err = %v, want ErrMailboxClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock pending control Enqueue within 1s")
	}
}

func TestBoundedControlAware_CloseIsIdempotent(t *testing.T) {
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 4, PushTimeout: time.Second})
	mb.Close()
	mb.Close() // must not panic on double-close
}

// ── Concurrency ─────────────────────────────────────────────────────────────

func TestBoundedControlAware_DequeueBlocksUntilEnqueue(t *testing.T) {
	mb := newBoundedControlAware(t, 4, time.Second)

	done := make(chan any, 1)
	go func() {
		done <- mb.Dequeue()
	}()
	select {
	case <-done:
		t.Fatal("Dequeue returned before any Enqueue")
	case <-time.After(20 * time.Millisecond):
	}

	if err := mb.Enqueue(testRegularMsg{id: 42}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case got := <-done:
		if got != (testRegularMsg{id: 42}) {
			t.Fatalf("Dequeue = %v, want testRegularMsg{id:42}", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Enqueue")
	}
}

func TestBoundedControlAware_ConcurrentProducersAndConsumers(t *testing.T) {
	// Capacity smaller than total work forces the bounded backpressure path
	// to engage; block-forever push-timeout keeps the test deterministic.
	mb := NewBoundedControlAwareFactory().NewMailbox(Config{Capacity: 32, PushTimeout: 0})

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
				var msg any
				if i%5 == 0 {
					msg = testControlMsg{id: base*perProducer + i}
				} else {
					msg = testRegularMsg{id: base*perProducer + i}
				}
				if err := mb.Enqueue(msg); err != nil {
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
					return
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

func TestRegistry_ResolveBoundedControlAware(t *testing.T) {
	cases := []string{
		"bounded-control-aware",
		"org.apache.pekko.dispatch.BoundedControlAwareMailbox",
		"akka.dispatch.BoundedControlAwareMailbox",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			f := Resolve(id)
			if f == nil {
				t.Fatalf("Resolve(%q) = nil, want bounded-control-aware factory", id)
			}
			mb := f.NewMailbox(Config{Capacity: 4, PushTimeout: time.Millisecond})
			if _, ok := mb.(*boundedControlAwareMailbox); !ok {
				t.Fatalf("Resolve(%q) factory produced %T, want *boundedControlAwareMailbox", id, mb)
			}
			mb.Close()
		})
	}
}

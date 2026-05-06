/*
 * control_aware_test.go
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

// ── Test fixtures ───────────────────────────────────────────────────────────

type testControlMsg struct{ id int }

func (testControlMsg) IsControlMessage() {}

type testRegularMsg struct{ id int }

func newUnboundedControlAware(t *testing.T) Mailbox {
	t.Helper()
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})
	t.Cleanup(mb.Close)
	return mb
}

// ── Control-message priority over regular ──────────────────────────────────

func TestUnboundedControlAware_ControlMessageDequeuesBefore1000Regulars(t *testing.T) {
	// Pre-load 1000 regular messages, THEN enqueue one control message.
	// A naive FIFO impl would dequeue regular #0 first; a true
	// drain-first impl returns the control message ahead of every
	// already-queued regular.
	mb := newUnboundedControlAware(t)
	for i := 0; i < 1000; i++ {
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

func TestUnboundedControlAware_DrainFirstIsDeterministic1000Trials(t *testing.T) {
	// Run 1000 trials of "control + regular both already queued"; the
	// dequeue must ALWAYS return control first. This proves the impl is
	// not the buggy naive 2-case select where Go picks randomly when both
	// channels are ready.
	const trials = 1000
	for i := 0; i < trials; i++ {
		mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})
		// Enqueue regular FIRST so any naive FIFO impl would mis-prioritise;
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

func TestUnboundedControlAware_FIFOWithinControlClass(t *testing.T) {
	mb := newUnboundedControlAware(t)
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

func TestUnboundedControlAware_FIFOWithinRegularClass(t *testing.T) {
	mb := newUnboundedControlAware(t)
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

// Drain order across priority classes: all control (in FIFO order) before
// all regular (in FIFO order), regardless of original interleaving.
func TestUnboundedControlAware_DrainOrderControlBeforeRegular(t *testing.T) {
	mb := newUnboundedControlAware(t)
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

// ── NumberOfMessages shape ──────────────────────────────────────────────────

func TestUnboundedControlAware_NumberOfMessagesCoversBothQueues(t *testing.T) {
	mb := newUnboundedControlAware(t)
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
	// Drain a control — count drops to 7.
	_ = mb.Dequeue()
	if got := mb.NumberOfMessages(); got != 7 {
		t.Fatalf("NumberOfMessages after 1 dequeue = %d, want 7", got)
	}
	// Drain another control — count drops to 6.
	_ = mb.Dequeue()
	if got := mb.NumberOfMessages(); got != 6 {
		t.Fatalf("NumberOfMessages after 2 dequeues = %d, want 6", got)
	}
}

// ── Validation ──────────────────────────────────────────────────────────────

func TestUnboundedControlAware_EnqueueNilRejected(t *testing.T) {
	mb := newUnboundedControlAware(t)
	err := mb.Enqueue(nil)
	if !errors.Is(err, ErrNilMessage) {
		t.Fatalf("Enqueue(nil) err = %v, want ErrNilMessage", err)
	}
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("rejected nil left %d messages, want 0", got)
	}
}

// ── Close semantics ─────────────────────────────────────────────────────────

func TestUnboundedControlAware_EnqueueAfterCloseRejected(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})
	mb.Close()
	if err := mb.Enqueue(testRegularMsg{id: 0}); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue regular after Close err = %v, want ErrMailboxClosed", err)
	}
	if err := mb.Enqueue(testControlMsg{id: 0}); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue control after Close err = %v, want ErrMailboxClosed", err)
	}
}

func TestUnboundedControlAware_CloseDrainsBothQueuesPreservingPriority(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})
	_ = mb.Enqueue(testRegularMsg{id: 1})
	_ = mb.Enqueue(testControlMsg{id: 2})
	_ = mb.Enqueue(testRegularMsg{id: 3})
	mb.Close()

	// Even after Close, queued messages remain dequeueable in priority
	// order (control first, then regular FIFO).
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

func TestUnboundedControlAware_CloseUnblocksWaitingDequeue(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})

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

func TestUnboundedControlAware_CloseIsIdempotent(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})
	mb.Close()
	mb.Close() // must not panic / double-broadcast crash
}

// ── Capacity/PushTimeout ignored (Pekko parity for unbounded) ───────────────

func TestUnboundedControlAware_CapacityAndPushTimeoutIgnored(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{
		Capacity:    1,
		PushTimeout: time.Nanosecond,
	})
	t.Cleanup(mb.Close)

	const N = 1000
	for i := 0; i < N; i++ {
		var msg any
		if i%3 == 0 {
			msg = testControlMsg{id: i}
		} else {
			msg = testRegularMsg{id: i}
		}
		if err := mb.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue(%d) on Config{Capacity:1}: %v", i, err)
		}
	}
	if got := mb.NumberOfMessages(); got != N {
		t.Fatalf("NumberOfMessages = %d, want %d (capacity must be ignored)", got, N)
	}
}

// ── Concurrency ─────────────────────────────────────────────────────────────

func TestUnboundedControlAware_DequeueBlocksUntilEnqueue(t *testing.T) {
	mb := newUnboundedControlAware(t)

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

func TestUnboundedControlAware_ConcurrentProducersAndConsumers(t *testing.T) {
	mb := NewUnboundedControlAwareFactory().NewMailbox(Config{})

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

func TestRegistry_ResolveUnboundedControlAware(t *testing.T) {
	cases := []string{
		"unbounded-control-aware",
		"org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
		"akka.dispatch.UnboundedControlAwareMailbox",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			f := Resolve(id)
			if f == nil {
				t.Fatalf("Resolve(%q) = nil, want unbounded-control-aware factory", id)
			}
			mb := f.NewMailbox(Config{})
			if _, ok := mb.(*unboundedControlAwareMailbox); !ok {
				t.Fatalf("Resolve(%q) factory produced %T, want *unboundedControlAwareMailbox", id, mb)
			}
			mb.Close()
		})
	}
}

// Ensure the local ControlMessage interface and a struct satisfying it
// classify correctly even via interface{} routing — guards against
// reflection-style mistakes in the type switch.
func TestUnboundedControlAware_ControlMessageDetectionViaInterface(t *testing.T) {
	mb := newUnboundedControlAware(t)
	var msg any = testControlMsg{id: 7}
	if _, ok := msg.(ControlMessage); !ok {
		t.Fatalf("testControlMsg does not satisfy ControlMessage marker")
	}
	if err := mb.Enqueue(msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got := mb.Dequeue()
	if got != (testControlMsg{id: 7}) {
		t.Fatalf("Dequeue = %v, want testControlMsg{id:7}", got)
	}
}

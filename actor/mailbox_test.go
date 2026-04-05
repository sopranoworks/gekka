/*
 * mailbox_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor_test

import (
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ── helper ───────────────────────────────────────────────────────────────────

// callbackActor invokes fn for each received message.
type callbackActor struct {
	actor.BaseActor
	fn func(msg any)
}

func (a *callbackActor) Receive(msg any) {
	if a.fn != nil {
		a.fn(msg)
	}
}

func newCallback(fn func(msg any)) *callbackActor {
	return &callbackActor{
		BaseActor: actor.NewBaseActor(),
		fn:        fn,
	}
}

func waitMessages(t *testing.T, mu *sync.Mutex, msgs *[]any, n int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		got := len(*msgs)
		mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout: received %d/%d messages", len(*msgs), n)
			mu.Unlock()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// ── P8-A: BoundedMailbox ──────────────────────────────────────────────────────

func TestBoundedMailbox_DropNewest_DeliversWithinCapacity(t *testing.T) {
	const cap = 5
	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewBoundedMailbox(cap, actor.DropNewest))
	actor.Start(a)

	for i := 0; i < cap; i++ {
		a.Send(i)
	}
	waitMessages(t, &mu, &received, cap)

	mu.Lock()
	n := len(received)
	mu.Unlock()
	if n != cap {
		t.Errorf("DropNewest: got %d messages, want %d", n, cap)
	}
}

func TestBoundedMailbox_DropNewest_DropsOverflow(t *testing.T) {
	const cap = 2
	ready := make(chan struct{})
	proceed := make(chan struct{})

	var mu sync.Mutex
	var received []any
	first := true

	a := newCallback(func(msg any) {
		if first {
			first = false
			close(ready)   // signal: actor is processing first message
			<-proceed      // hold actor until we've saturated the mailbox
		}
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewBoundedMailbox(cap, actor.DropNewest))
	actor.Start(a)

	// Wait for actor to start processing, then fill mailbox + overflow.
	a.Send(0) // triggers actor; it will block on <-proceed
	<-ready
	for i := 1; i <= cap+5; i++ {
		a.Send(i) // cap slots fill, extras dropped
	}
	close(proceed) // release actor

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	n := len(received)
	mu.Unlock()
	// Actor processed msg 0; then at most `cap` buffered messages.
	if n > cap+1 {
		t.Errorf("DropNewest: expected at most %d delivered, got %d", cap+1, n)
	}
}

func TestBoundedMailbox_DropOldest_KeepsNewest(t *testing.T) {
	// Freeze the actor while we overflow the mailbox.
	ready := make(chan struct{})
	proceed := make(chan struct{})
	first := true

	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		if first {
			first = false
			close(ready)
			<-proceed
		}
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	const cap = 2
	actor.InjectMailbox(a, actor.NewBoundedMailbox(cap, actor.DropOldest))
	actor.Start(a)

	a.Send(0) // triggers first Receive (holds on <-proceed)
	<-ready
	// Fill mailbox: 1, 2 fill the 2 slots.
	// Then 3, 4, 5 each evict the oldest in the buffer.
	for i := 1; i <= 5; i++ {
		a.Send(i)
	}
	close(proceed)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := make([]any, len(received))
	copy(got, received)
	mu.Unlock()

	// msg 5 (newest) must have been delivered.
	found := false
	for _, v := range got {
		if v.(int) == 5 {
			found = true
		}
	}
	if !found {
		t.Errorf("DropOldest: newest message (5) not received; got %v", got)
	}
}

func TestBoundedMailbox_BackPressure_AllMessagesDelivered(t *testing.T) {
	const total = 5
	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewBoundedMailbox(1, actor.BackPressure))
	actor.Start(a)

	// BackPressure: sender blocks when mailbox is full; no messages are dropped.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			a.Send(i)
		}
	}()
	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	n := len(received)
	mu.Unlock()
	if n != total {
		t.Errorf("BackPressure: got %d messages, want %d", n, total)
	}
}

func TestBoundedMailbox_CloseMailbox_NoDeadlock(t *testing.T) {
	a := newCallback(func(msg any) {})
	actor.InjectMailbox(a, actor.NewBoundedMailbox(8, actor.DropNewest))
	actor.Start(a)
	a.Send(1)
	time.Sleep(20 * time.Millisecond)
	a.CloseMailbox()
	time.Sleep(30 * time.Millisecond)
}

// ── Props.Mailbox integration ─────────────────────────────────────────────────

// TestProps_Mailbox_InjectMailboxWiring confirms InjectMailbox wires the factory
// correctly (same hook used by ActorOf / SpawnActor when Props.Mailbox is set).
func TestProps_Mailbox_InjectMailboxWiring(t *testing.T) {
	var mu sync.Mutex
	var received []any
	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})

	actor.InjectMailbox(a, actor.NewBoundedMailbox(4, actor.DropNewest))
	actor.Start(a)

	for i := 0; i < 4; i++ {
		a.Send(i)
	}
	waitMessages(t, &mu, &received, 4)

	mu.Lock()
	n := len(received)
	mu.Unlock()
	if n != 4 {
		t.Errorf("got %d messages, want 4", n)
	}
}

// ── P8-B: UnboundedPriorityMailbox ───────────────────────────────────────────

// TestPriorityMailbox_HighPriorityFirst loads all messages into the heap BEFORE
// starting the actor's receive goroutine, so the drain goroutine has a fully
// populated heap to sort through and always picks the minimum-value item.
func TestPriorityMailbox_HighPriorityFirst(t *testing.T) {
	less := func(a, b any) bool { return a.(int) < b.(int) }

	var mu sync.Mutex
	var received []int
	allDone := make(chan struct{})
	const total = 5

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg.(int))
		if len(received) == total {
			close(allDone)
		}
		mu.Unlock()
	})

	// InjectMailbox starts the drain goroutine; call Start AFTER sending so
	// all messages are in the heap before the actor goroutine reads from out.
	actor.InjectMailbox(a, actor.NewPriorityMailbox(less))

	// Enqueue in non-priority order: 5, 3, 1, 4, 2.
	// The drain goroutine is running but cannot write to out (actor not started).
	for _, v := range []int{5, 3, 1, 4, 2} {
		a.Send(v)
	}
	// Give the drain goroutine a moment to settle on the highest-priority item.
	time.Sleep(10 * time.Millisecond)

	actor.Start(a) // actor goroutine begins reading from out

	select {
	case <-allDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for all priority messages")
	}

	mu.Lock()
	got := make([]int, len(received))
	copy(got, received)
	mu.Unlock()

	// With all 5 messages in the heap before the first read, they must be
	// delivered in ascending order (smallest int = highest priority).
	want := []int{1, 2, 3, 4, 5}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("priority order violated: got %v, want %v", got, want)
			break
		}
	}
}

// TestPriorityMailbox_SwapsOnHigherPriorityArrival verifies that if the drain
// goroutine is blocked writing item X to the actor, and a higher-priority item Y
// arrives, the actor receives Y before X.
func TestPriorityMailbox_SwapsOnHigherPriorityArrival(t *testing.T) {
	less := func(a, b any) bool { return a.(int) < b.(int) }

	var mu sync.Mutex
	var received []int

	gate := make(chan struct{})
	first := true
	allDone := make(chan struct{})

	a := newCallback(func(msg any) {
		if first {
			first = false
			<-gate // hold actor after first message
		}
		mu.Lock()
		received = append(received, msg.(int))
		if len(received) == 3 {
			close(allDone)
		}
		mu.Unlock()
	})

	actor.InjectMailbox(a, actor.NewPriorityMailbox(less))
	actor.Start(a)

	// Send a low-priority item first; drain goroutine picks it up.
	a.Send(10) // actor processes this immediately
	time.Sleep(5 * time.Millisecond) // let actor start (holds on <-gate)

	// Now send a higher-priority item; drain goroutine should swap.
	a.Send(3)
	a.Send(1) // higher priority than 3 and 10
	time.Sleep(5 * time.Millisecond)

	close(gate) // release actor

	select {
	case <-allDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	got := make([]int, len(received))
	copy(got, received)
	mu.Unlock()

	// After 10 is received (before gate opens), items 3 and 1 are in heap.
	// Next should be 1 (highest priority), then 3.
	if len(got) < 3 {
		t.Fatalf("expected 3 messages, got %d: %v", len(got), got)
	}
	if got[0] != 10 {
		t.Errorf("first message should be 10, got %d", got[0])
	}
	if got[1] != 1 {
		t.Errorf("second message should be 1 (highest priority), got %d; full: %v", got[1], got)
	}
	if got[2] != 3 {
		t.Errorf("third message should be 3, got %d", got[2])
	}
}

func TestPriorityMailbox_AllMessagesDelivered(t *testing.T) {
	const total = 100
	less := func(a, b any) bool { return a.(int) < b.(int) }

	var mu sync.Mutex
	var received []int
	allDone := make(chan struct{})

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg.(int))
		if len(received) == total {
			close(allDone)
		}
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewPriorityMailbox(less))
	actor.Start(a)

	for i := 0; i < total; i++ {
		a.Send(i)
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		mu.Lock()
		t.Fatalf("timeout: received %d/%d", len(received), total)
		mu.Unlock()
	}

	mu.Lock()
	n := len(received)
	mu.Unlock()
	if n != total {
		t.Errorf("expected %d messages, got %d", total, n)
	}
}

func TestPriorityMailbox_CloseMailbox_StopsCleanly(t *testing.T) {
	less := func(a, b any) bool { return a.(int) < b.(int) }
	a := newCallback(func(msg any) {})
	actor.InjectMailbox(a, actor.NewPriorityMailbox(less))
	actor.Start(a)
	a.Send(1)
	time.Sleep(20 * time.Millisecond)
	a.CloseMailbox()
	time.Sleep(50 * time.Millisecond)
}

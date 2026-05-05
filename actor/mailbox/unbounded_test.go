/*
 * unbounded_test.go
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

func newUnbounded(t *testing.T) Mailbox {
	t.Helper()
	mb := NewUnboundedFactory().NewMailbox(Config{})
	t.Cleanup(mb.Close)
	return mb
}

func TestUnbounded_FIFOOrder(t *testing.T) {
	mb := newUnbounded(t)
	for i := 0; i < 10; i++ {
		if err := mb.Enqueue(i); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		got := mb.Dequeue()
		if got != i {
			t.Fatalf("Dequeue #%d = %v, want %d", i, got, i)
		}
	}
}

func TestUnbounded_NumberOfMessages(t *testing.T) {
	mb := newUnbounded(t)
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

func TestUnbounded_EnqueueNilRejected(t *testing.T) {
	mb := newUnbounded(t)
	err := mb.Enqueue(nil)
	if !errors.Is(err, ErrNilMessage) {
		t.Fatalf("Enqueue(nil) err = %v, want ErrNilMessage", err)
	}
	if got := mb.NumberOfMessages(); got != 0 {
		t.Fatalf("rejected nil left %d messages, want 0", got)
	}
}

func TestUnbounded_EnqueueAfterCloseRejected(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})
	mb.Close()
	err := mb.Enqueue("late")
	if !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Enqueue after Close err = %v, want ErrMailboxClosed", err)
	}
}

func TestUnbounded_CloseDrainsBufferedMessages(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})
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

func TestUnbounded_CloseUnblocksWaitingDequeue(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})

	done := make(chan any, 1)
	go func() {
		done <- mb.Dequeue()
	}()

	// Give the goroutine time to enter the cond.Wait().
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

func TestUnbounded_DequeueBlocksUntilEnqueue(t *testing.T) {
	mb := newUnbounded(t)

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

func TestUnbounded_CloseIsIdempotent(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})
	mb.Close()
	mb.Close() // must not panic / double-broadcast crash
}

func TestUnbounded_CapacityAndPushTimeoutIgnored(t *testing.T) {
	// Pekko's UnboundedMailbox is a LinkedBlockingQueue with no capacity,
	// so even pathological Config values must not constrain the queue.
	mb := NewUnboundedFactory().NewMailbox(Config{
		Capacity:    1,
		PushTimeout: time.Nanosecond,
	})
	t.Cleanup(mb.Close)

	const N = 1000
	for i := 0; i < N; i++ {
		if err := mb.Enqueue(i); err != nil {
			t.Fatalf("Enqueue(%d) on Config{Capacity:1}: %v", i, err)
		}
	}
	if got := mb.NumberOfMessages(); got != N {
		t.Fatalf("NumberOfMessages = %d, want %d (capacity must be ignored)", got, N)
	}
}

func TestUnbounded_ConcurrentProducersConsumer(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})

	const producers = 8
	const perProducer = 1000
	const total = producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := mb.Enqueue(base*perProducer + i); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	seen := make(map[int]struct{}, total)
	for i := 0; i < total; i++ {
		v, ok := mb.Dequeue().(int)
		if !ok {
			t.Fatalf("Dequeue #%d non-int %v", i, v)
		}
		if _, dup := seen[v]; dup {
			t.Fatalf("Dequeue produced duplicate %d", v)
		}
		seen[v] = struct{}{}
	}
	mb.Close()
	if extra := mb.Dequeue(); extra != nil {
		t.Fatalf("post-close Dequeue extra = %v, want nil", extra)
	}
}

func TestUnbounded_ConcurrentProducersAndConsumers(t *testing.T) {
	mb := NewUnboundedFactory().NewMailbox(Config{})

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

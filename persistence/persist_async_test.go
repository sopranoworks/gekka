/*
 * persist_async_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── gatedJournal ─────────────────────────────────────────────────────────────

// gatedJournal wraps an InMemoryJournal and blocks AsyncWriteMessages until
// gate is closed. Used to simulate a slow journal so we can observe that
// commands are processed before any journal ack.
type gatedJournal struct {
	inner *InMemoryJournal
	gate  <-chan struct{}
}

func (g *gatedJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	<-g.gate // block until gate is opened
	return g.inner.AsyncWriteMessages(ctx, messages)
}
func (g *gatedJournal) ReplayMessages(ctx context.Context, id string, from, to, max uint64, cb func(PersistentRepr)) error {
	return g.inner.ReplayMessages(ctx, id, from, to, max, cb)
}
func (g *gatedJournal) ReadHighestSequenceNr(ctx context.Context, id string, from uint64) (uint64, error) {
	return g.inner.ReadHighestSequenceNr(ctx, id, from)
}
func (g *gatedJournal) AsyncDeleteMessagesTo(ctx context.Context, id string, to uint64) error {
	return g.inner.AsyncDeleteMessagesTo(ctx, id, to)
}

// ── test domain ───────────────────────────────────────────────────────────────

type asyncIncrCmd struct{ seq int }
type asyncIncrEvent struct{ seq int }

// asyncIncrActor wraps PersistentFSM and calls PersistAsync for each command.
type asyncIncrActor struct {
	*PersistentFSM[int, int, asyncIncrEvent]
	onHandled func(asyncIncrEvent)
}

func newAsyncIncrActor(j Journal, onHandled func(asyncIncrEvent)) *asyncIncrActor {
	fsm := NewPersistentFSM[int, int, asyncIncrEvent]("async-incr", j)
	fsm.StartWith(0, 0)
	fsm.ApplyEvent(func(s, d int, e asyncIncrEvent) (int, int) {
		return s + 1, d
	})
	// Suppress "unhandled message" warning for asyncIncrCmd.
	fsm.WhenUnhandled(func(e FSMEvent[int]) FSMStateResult[int, int, asyncIncrEvent] {
		return fsm.Stay().Build()
	})
	return &asyncIncrActor{PersistentFSM: fsm, onHandled: onHandled}
}

func (a *asyncIncrActor) Receive(msg any) {
	// Let FSM intercept persistAsyncAck and call our handlers.
	a.PersistentFSM.Receive(msg)

	// For user commands, issue the async persist.
	if cmd, ok := msg.(asyncIncrCmd); ok {
		event := asyncIncrEvent{seq: cmd.seq}
		a.PersistAsync(event, a.onHandled)
	}
}

// drainMailbox reads pending messages from the actor's mailbox and delivers
// them via Receive, up to limit messages or timeout.
func drainMailbox(a *asyncIncrActor, limit int, timeout time.Duration) int {
	deadline := time.After(timeout)
	received := 0
	for received < limit {
		select {
		case msg := <-a.Mailbox():
			a.Receive(msg)
			received++
		case <-deadline:
			return received
		}
	}
	return received
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestPersistAsync_OrderPreserved verifies that async-persist handlers are
// invoked in the same order as the original PersistAsync calls.
func TestPersistAsync_OrderPreserved(t *testing.T) {
	const n = 10
	journal := NewInMemoryJournal()

	var mu sync.Mutex
	var order []int

	a := newAsyncIncrActor(journal, func(e asyncIncrEvent) {
		mu.Lock()
		order = append(order, e.seq)
		mu.Unlock()
	})
	a.PersistentFSMPreStart()

	// Issue n PersistAsync calls.
	for i := 0; i < n; i++ {
		a.Receive(asyncIncrCmd{seq: i})
	}

	// Drain all n acks from the mailbox.
	got := drainMailbox(a, n, 5*time.Second)
	require.Equal(t, n, got, "expected %d acks", n)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, order, n)
	for i, v := range order {
		assert.Equal(t, i, v, "handler[%d] should have seq=%d, got %d", i, i, v)
	}
}

// TestPersistAsync_CommandsProcessedBeforeAck is the high-throughput scenario:
// 100 commands arrive and are fully processed by the actor before any journal
// ack comes back (the gated journal blocks writes until we signal it).
func TestPersistAsync_CommandsProcessedBeforeAck(t *testing.T) {
	const n = 100
	gate := make(chan struct{})
	inner := NewInMemoryJournal()
	journal := &gatedJournal{inner: inner, gate: gate}

	var handledMu sync.Mutex
	var handled []int

	a := newAsyncIncrActor(journal, func(e asyncIncrEvent) {
		handledMu.Lock()
		handled = append(handled, e.seq)
		handledMu.Unlock()
	})
	a.PersistentFSMPreStart()

	// Process all n commands — gate is still closed, so no acks have arrived.
	for i := 0; i < n; i++ {
		a.Receive(asyncIncrCmd{seq: i})
	}

	// Verify: handlers have NOT been called yet.
	handledMu.Lock()
	assert.Empty(t, handled, "no handlers should be called while gate is closed")
	handledMu.Unlock()

	// Open gate: allow background goroutine to write and send acks.
	close(gate)

	// Drain all n acks.
	got := drainMailbox(a, n, 5*time.Second)
	require.Equal(t, n, got, "expected %d ack messages", n)

	// Verify all handlers were called in order.
	handledMu.Lock()
	defer handledMu.Unlock()
	require.Len(t, handled, n)
	for i, v := range handled {
		assert.Equal(t, i, v, "handler[%d] out of order", i)
	}

	// Verify all events are in the journal.
	seqNr, err := inner.ReadHighestSequenceNr(context.Background(), "async-incr", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(n), seqNr)
}

// TestPersistAllAsync_BatchPersist verifies that PersistAllAsync persists a
// batch of events and invokes the handler once per event, in order.
func TestPersistAllAsync_BatchPersist(t *testing.T) {
	const n = 5
	journal := NewInMemoryJournal()

	var mu sync.Mutex
	var handled []int

	fsm := NewPersistentFSM[int, int, asyncIncrEvent]("async-batch", journal)
	fsm.StartWith(0, 0)
	fsm.ApplyEvent(func(s, d int, e asyncIncrEvent) (int, int) { return s + 1, d })
	fsm.WhenUnhandled(func(e FSMEvent[int]) FSMStateResult[int, int, asyncIncrEvent] {
		return fsm.Stay().Build()
	})
	fsm.PersistentFSMPreStart()

	events := make([]asyncIncrEvent, n)
	for i := range events {
		events[i] = asyncIncrEvent{seq: i}
	}

	fsm.PersistAllAsync(events, func(e asyncIncrEvent) {
		mu.Lock()
		handled = append(handled, e.seq)
		mu.Unlock()
	})

	// Drain the single batch ack (PersistAllAsync sends one task → one ack).
	deadline := time.After(5 * time.Second)
	select {
	case msg := <-fsm.Mailbox():
		fsm.Receive(msg)
	case <-deadline:
		t.Fatal("timed out waiting for PersistAllAsync ack")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, handled, n)
	for i, v := range handled {
		assert.Equal(t, i, v, "PersistAllAsync handler[%d] out of order", i)
	}

	// Verify journal contains all n events.
	seqNr, err := journal.ReadHighestSequenceNr(context.Background(), "async-batch", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(n), seqNr)
}

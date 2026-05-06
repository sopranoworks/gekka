/*
 * control_aware.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

func init() {
	Register(unboundedControlAwareFactory{},
		"unbounded-control-aware",
		"org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
		"akka.dispatch.UnboundedControlAwareMailbox",
	)
	Register(boundedControlAwareFactory{},
		"bounded-control-aware",
		"org.apache.pekko.dispatch.BoundedControlAwareMailbox",
		"akka.dispatch.BoundedControlAwareMailbox",
	)
}

// ControlMessage is the marker interface for messages that should be
// dispatched ahead of regular user messages by a control-aware mailbox.
//
// The shape mirrors actor.ControlMessage exactly — any type that satisfies
// one satisfies the other. The interface is duplicated here so the mailbox
// package can classify messages without importing the parent actor package,
// which would create a cycle once the actor cell wires Mailbox in
// sub-commit 1.6. The two interfaces will be unified at that point.
type ControlMessage interface {
	IsControlMessage()
}

// NewUnboundedControlAwareFactory returns the factory for unbounded
// control-aware mailboxes. Direct callers (tests, custom Props) can use
// this in lieu of the registry.
func NewUnboundedControlAwareFactory() MailboxFactory {
	return unboundedControlAwareFactory{}
}

type unboundedControlAwareFactory struct{}

// NewMailbox creates an UnboundedControlAwareMailbox. Capacity and
// PushTimeout in cfg are intentionally ignored — Pekko's
// UnboundedControlAwareMailbox is backed by two LinkedBlockingDeques and
// never refuses or blocks senders. The bounded variant lands in sub-commit
// 1.5 with its own factory.
func (unboundedControlAwareFactory) NewMailbox(_ Config) Mailbox {
	return &unboundedControlAwareMailbox{chans: newPriorityChans()}
}

// priorityChans is the shared two-queue priority dispatcher used by the
// control-aware mailbox variants. The drain-first contract — the control
// queue is checked before the regular queue on every Dequeue, regardless
// of arrival order — is enforced here so both
// UnboundedControlAwareMailbox and BoundedControlAwareMailbox (sub-commit
// 1.5) inherit the same semantics.
//
// Both queues are list-backed under a single sync.Cond, mirroring the
// pattern in unbounded.go. Sub-commit 1.5 will add a parallel
// boundedPriorityChans backed by buffered channels with per-side
// push-timeout — they share this file but not this struct.
//
// The list+cond layout was chosen over the plan's pseudo-code two-channel
// select because Go channels have a fixed capacity and the unbounded
// variant must accept arbitrarily many messages without blocking — the
// same constraint that drove unbounded.go. Drain-first determinism is
// preserved by always checking control.Front() before regular.Front()
// inside Dequeue, which is functionally equivalent to the plan's
// non-blocking select on the priority channel.
type priorityChans struct {
	mu      sync.Mutex
	cond    *sync.Cond
	control *list.List
	regular *list.List
	closed  bool
}

func newPriorityChans() *priorityChans {
	p := &priorityChans{
		control: list.New(),
		regular: list.New(),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// enqueue routes msg by ControlMessage marker. Returns ErrNilMessage on
// nil input and ErrMailboxClosed if the mailbox has been closed.
func (p *priorityChans) enqueue(msg any) error {
	if msg == nil {
		return ErrNilMessage
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrMailboxClosed
	}
	if _, isCtrl := msg.(ControlMessage); isCtrl {
		p.control.PushBack(msg)
	} else {
		p.regular.PushBack(msg)
	}
	p.mu.Unlock()
	p.cond.Signal()
	return nil
}

// dequeue blocks until either queue has a message, then drains the
// control queue first. Returns nil only when the mailbox has been closed
// and both queues are empty.
func (p *priorityChans) dequeue() any {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.control.Len() == 0 && p.regular.Len() == 0 && !p.closed {
		p.cond.Wait()
	}
	if e := p.control.Front(); e != nil {
		p.control.Remove(e)
		return e.Value
	}
	if e := p.regular.Front(); e != nil {
		p.regular.Remove(e)
		return e.Value
	}
	return nil
}

// numberOfMessages returns the snapshot total across both queues.
// Best-effort under concurrent producers — same caveat as Pekko's
// MessageQueue.numberOfMessages.
func (p *priorityChans) numberOfMessages() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.control.Len() + p.regular.Len()
}

// close marks the mailbox closed and wakes every waiter. Idempotent.
func (p *priorityChans) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	p.cond.Broadcast()
}

// unboundedControlAwareMailbox is the public Mailbox impl. It is a thin
// wrapper over priorityChans so the bounded variant in sub-commit 1.5 can
// reuse the same priority semantics with a different storage strategy.
type unboundedControlAwareMailbox struct {
	chans *priorityChans
}

func (m *unboundedControlAwareMailbox) Enqueue(msg any) error { return m.chans.enqueue(msg) }
func (m *unboundedControlAwareMailbox) Dequeue() any          { return m.chans.dequeue() }
func (m *unboundedControlAwareMailbox) NumberOfMessages() int { return m.chans.numberOfMessages() }
func (m *unboundedControlAwareMailbox) Close()                { m.chans.close() }

// ── BoundedControlAwareMailbox ──────────────────────────────────────────────
//
// Sub-commit 1.5 adds the bounded counterpart of the unbounded control-aware
// mailbox. It shares the ControlMessage classifier and the drain-first
// dispatch contract with the unbounded variant, but the underlying storage is
// two buffered Go channels — one per priority class — rather than the
// unbounded list+cond used above. The two storage strategies differ enough
// that they live in parallel structs (priorityChans vs boundedPriorityChans)
// rather than sharing one.
//
// Per-side push-timeout is implemented with select-plus-time.After, mirroring
// bounded.go. Close uses a dedicated `done` channel guarded by sync.Once; the
// data channels are never closed (closing a buffered channel with in-flight
// senders panics), exactly the pattern bounded.go documents.

// NewBoundedControlAwareFactory returns the factory for bounded
// control-aware mailboxes. Direct callers (tests, custom Props) can use
// this in lieu of the registry.
func NewBoundedControlAwareFactory() MailboxFactory { return boundedControlAwareFactory{} }

type boundedControlAwareFactory struct{}

// NewMailbox creates a BoundedControlAwareMailbox with two buffered channels,
// each sized to cfg.Capacity. Pekko's BoundedControlAwareMailbox uses a
// single capacity for both queues; gekka follows that convention. cfg.Capacity
// must be > 0; the factory panics otherwise, matching Pekko's
// BoundedControlAwareMailbox whose underlying ArrayBlockingQueue rejects
// non-positive capacity at construction.
//
// cfg.PushTimeout is the per-Enqueue timeout applied to whichever side the
// message is routed to. A zero or negative value means the sender blocks
// until space opens up or the mailbox closes (Pekko parity with
// mailbox-push-timeout-time = 0s).
func (boundedControlAwareFactory) NewMailbox(cfg Config) Mailbox {
	if cfg.Capacity <= 0 {
		panic(fmt.Sprintf("mailbox: BoundedControlAwareMailbox requires Capacity > 0, got %d", cfg.Capacity))
	}
	return &boundedControlAwareMailbox{
		chans: &boundedPriorityChans{
			controlCh:   make(chan any, cfg.Capacity),
			regularCh:   make(chan any, cfg.Capacity),
			pushTimeout: cfg.PushTimeout,
			done:        make(chan struct{}),
		},
	}
}

// boundedPriorityChans is the bounded counterpart of priorityChans. The two
// queues are buffered Go channels rather than lists; the drain-first
// dequeue contract is preserved via the canonical "non-blocking control
// first, then blocking select on either" pattern. Both channels share a
// single `done` sentinel so Close fires both senders and dequeuers
// simultaneously without ever closing the data channels.
type boundedPriorityChans struct {
	controlCh   chan any
	regularCh   chan any
	pushTimeout time.Duration
	closeOnce   sync.Once
	done        chan struct{}
}

// enqueue routes msg by the ControlMessage marker into the matching channel,
// honouring the configured per-side push-timeout. Returns ErrNilMessage on
// nil input, ErrMailboxClosed if the mailbox has been closed, and
// ErrMailboxFull if the timeout elapses before space opens up.
func (q *boundedPriorityChans) enqueue(msg any) error {
	if msg == nil {
		return ErrNilMessage
	}
	if _, isCtrl := msg.(ControlMessage); isCtrl {
		return q.enqueueOn(q.controlCh, msg)
	}
	return q.enqueueOn(q.regularCh, msg)
}

// enqueueOn applies the bounded push-timeout protocol to a single side.
// Same shape as boundedMailbox.Enqueue: fast non-blocking close check,
// non-blocking send fast path when space is available, then a timer-armed
// select that races the send, close, and timeout.
func (q *boundedPriorityChans) enqueueOn(ch chan any, msg any) error {
	select {
	case <-q.done:
		return ErrMailboxClosed
	default:
	}

	if q.pushTimeout <= 0 {
		select {
		case ch <- msg:
			return nil
		case <-q.done:
			return ErrMailboxClosed
		}
	}

	// Non-blocking fast path so the timer is never armed when space is
	// already available.
	select {
	case ch <- msg:
		return nil
	default:
	}

	timer := time.NewTimer(q.pushTimeout)
	defer timer.Stop()
	select {
	case ch <- msg:
		return nil
	case <-q.done:
		return ErrMailboxClosed
	case <-timer.C:
		return ErrMailboxFull
	}
}

// dequeue blocks until either channel has a message, then drains the control
// channel first. Returns nil only when the mailbox is closed and both
// channels are fully drained.
//
// The two-phase select (non-blocking control first, then blocking three-way)
// is the canonical drain-first dispatch pattern. The non-blocking phase is
// load-bearing: if both channels have messages by the time Dequeue is
// called, Go's blocking select would pick uniformly at random. Trying
// control-only first guarantees deterministic priority for that scenario,
// which is exactly the case the non-determinism guard test exercises.
func (q *boundedPriorityChans) dequeue() any {
	// Drain-first: pending control message takes absolute priority over
	// anything that may simultaneously be ready on the regular channel.
	select {
	case msg := <-q.controlCh:
		return msg
	default:
	}

	select {
	case msg := <-q.controlCh:
		return msg
	case msg := <-q.regularCh:
		return msg
	case <-q.done:
		// Closed — drain residual messages preserving control-first
		// priority before returning the closed-and-drained sentinel.
		select {
		case msg := <-q.controlCh:
			return msg
		default:
		}
		select {
		case msg := <-q.regularCh:
			return msg
		default:
			return nil
		}
	}
}

// numberOfMessages is a best-effort snapshot across both buffered channels.
// Racy under concurrent producers/consumers — same caveat as Pekko's
// MessageQueue.numberOfMessages.
func (q *boundedPriorityChans) numberOfMessages() int {
	return len(q.controlCh) + len(q.regularCh)
}

// close marks the mailbox closed by signalling the shared done channel.
// Idempotent; never closes the data channels (which would panic any
// in-flight sender). Pending Enqueue calls observe the close via select
// and return ErrMailboxClosed; Dequeue keeps draining residual messages
// then returns the nil sentinel.
func (q *boundedPriorityChans) close() {
	q.closeOnce.Do(func() {
		close(q.done)
	})
}

// boundedControlAwareMailbox is the public Mailbox impl. It wraps a
// boundedPriorityChans so the actor cell sees the same Mailbox surface as
// the unbounded variant.
type boundedControlAwareMailbox struct {
	chans *boundedPriorityChans
}

func (m *boundedControlAwareMailbox) Enqueue(msg any) error { return m.chans.enqueue(msg) }
func (m *boundedControlAwareMailbox) Dequeue() any          { return m.chans.dequeue() }
func (m *boundedControlAwareMailbox) NumberOfMessages() int { return m.chans.numberOfMessages() }
func (m *boundedControlAwareMailbox) Close()                { m.chans.close() }

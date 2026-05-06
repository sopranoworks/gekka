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
	"sync"
)

func init() {
	f := unboundedControlAwareFactory{}
	Register(f,
		"unbounded-control-aware",
		"org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
		"akka.dispatch.UnboundedControlAwareMailbox",
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

func (m *unboundedControlAwareMailbox) Enqueue(msg any) error  { return m.chans.enqueue(msg) }
func (m *unboundedControlAwareMailbox) Dequeue() any           { return m.chans.dequeue() }
func (m *unboundedControlAwareMailbox) NumberOfMessages() int  { return m.chans.numberOfMessages() }
func (m *unboundedControlAwareMailbox) Close()                 { m.chans.close() }

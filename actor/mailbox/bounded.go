/*
 * bounded.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"fmt"
	"sync"
	"time"
)

func init() {
	f := boundedFactory{}
	Register(f,
		"bounded",
		"org.apache.pekko.dispatch.BoundedMailbox",
		"akka.dispatch.BoundedMailbox",
	)
}

// NewBoundedFactory returns the factory for bounded mailboxes. Direct callers
// (tests, custom Props) can use this in lieu of the registry.
func NewBoundedFactory() MailboxFactory { return boundedFactory{} }

type boundedFactory struct{}

// NewMailbox creates a BoundedMailbox sized to cfg.Capacity. cfg.PushTimeout
// is the per-Enqueue timeout: a positive value bounds how long a sender
// waits for free space before Enqueue returns ErrMailboxFull. A zero or
// negative timeout means the sender blocks until space opens up or the
// mailbox is closed (mirrors Pekko's BoundedMailbox with
// mailbox-push-timeout-time = 0s — block-forever).
//
// cfg.Capacity must be > 0; the factory panics otherwise. This matches
// Pekko's BoundedMailbox, whose underlying ArrayBlockingQueue rejects
// non-positive capacity at construction.
func (boundedFactory) NewMailbox(cfg Config) Mailbox {
	if cfg.Capacity <= 0 {
		panic(fmt.Sprintf("mailbox: BoundedMailbox requires Capacity > 0, got %d", cfg.Capacity))
	}
	return &boundedMailbox{
		ch:          make(chan any, cfg.Capacity),
		capacity:    cfg.Capacity,
		pushTimeout: cfg.PushTimeout,
		done:        make(chan struct{}),
	}
}

// boundedMailbox is a capacity-capped FIFO queue backed by a buffered Go
// channel. The channel itself is never closed — closing a channel with
// in-flight senders panics. Instead, Close signals the dedicated `done`
// channel; senders watch it via select, while Dequeue keeps draining
// already-buffered messages until empty before returning the
// closed-and-drained nil sentinel.
type boundedMailbox struct {
	ch          chan any
	capacity    int
	pushTimeout time.Duration
	closeOnce   sync.Once
	done        chan struct{}
}

// Enqueue inserts msg, honouring the configured push-timeout. Returns
// ErrNilMessage on nil input, ErrMailboxClosed if the mailbox has been
// closed, ErrMailboxFull if the timeout elapses before space opens up.
func (m *boundedMailbox) Enqueue(msg any) error {
	if msg == nil {
		return ErrNilMessage
	}
	// Fast non-blocking close check so a closed mailbox cannot accept new
	// work even if `m.ch` happens to have buffered space.
	select {
	case <-m.done:
		return ErrMailboxClosed
	default:
	}

	if m.pushTimeout <= 0 {
		// Block until space opens up or the mailbox closes.
		select {
		case m.ch <- msg:
			return nil
		case <-m.done:
			return ErrMailboxClosed
		}
	}

	// Try non-blocking send first to avoid the timer allocation on the hot
	// path when the mailbox is not full.
	select {
	case m.ch <- msg:
		return nil
	default:
	}

	timer := time.NewTimer(m.pushTimeout)
	defer timer.Stop()
	select {
	case m.ch <- msg:
		return nil
	case <-m.done:
		return ErrMailboxClosed
	case <-timer.C:
		return ErrMailboxFull
	}
}

// Dequeue blocks until a message is available, returning nil only when the
// mailbox has been closed and fully drained. Already-buffered messages
// remain visible after Close so the actor cell can finish processing
// in-flight work before terminating.
func (m *boundedMailbox) Dequeue() any {
	// Buffered-first priority: drain pending messages even after Close.
	select {
	case msg := <-m.ch:
		return msg
	default:
	}
	select {
	case msg := <-m.ch:
		return msg
	case <-m.done:
		// Done was signalled while we were waiting — drain any straggler
		// that snuck in before the close, otherwise return the
		// closed-and-drained sentinel.
		select {
		case msg := <-m.ch:
			return msg
		default:
			return nil
		}
	}
}

// NumberOfMessages is a best-effort snapshot. Under concurrent producers
// and consumers it is racy by design — the same caveat as Pekko's
// MessageQueue.numberOfMessages.
func (m *boundedMailbox) NumberOfMessages() int {
	return len(m.ch)
}

// Close marks the mailbox closed. Subsequent Enqueue calls fail; pending
// Dequeue calls drain the buffered residue and then return the
// closed-and-drained sentinel. Idempotent.
func (m *boundedMailbox) Close() {
	m.closeOnce.Do(func() {
		close(m.done)
	})
}

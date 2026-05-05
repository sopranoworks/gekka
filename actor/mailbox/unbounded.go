/*
 * unbounded.go
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
	f := unboundedFactory{}
	Register(f,
		DefaultMailboxID,
		"org.apache.pekko.dispatch.UnboundedMailbox",
		"akka.dispatch.UnboundedMailbox",
	)
}

// NewUnboundedFactory returns the factory for unbounded mailboxes. Direct
// callers (tests, custom Props) can use this in lieu of the registry.
func NewUnboundedFactory() MailboxFactory { return unboundedFactory{} }

type unboundedFactory struct{}

// NewMailbox creates an UnboundedMailbox. Capacity and PushTimeout in cfg
// are intentionally ignored — Pekko's UnboundedMailbox is backed by an
// unbounded LinkedBlockingQueue and never refuses or blocks senders.
func (unboundedFactory) NewMailbox(_ Config) Mailbox {
	m := &unboundedMailbox{queue: list.New()}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// unboundedMailbox is a true-unbounded FIFO queue. Reads block on empty via
// sync.Cond; writes never block. container/list gives O(1) push/pop without
// the slice-trim memory-retention pitfall that a `q = q[1:]` queue has.
type unboundedMailbox struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  *list.List
	closed bool
}

func (m *unboundedMailbox) Enqueue(msg any) error {
	if msg == nil {
		return ErrNilMessage
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrMailboxClosed
	}
	m.queue.PushBack(msg)
	m.mu.Unlock()
	m.cond.Signal()
	return nil
}

func (m *unboundedMailbox) Dequeue() any {
	m.mu.Lock()
	defer m.mu.Unlock()
	for m.queue.Len() == 0 && !m.closed {
		m.cond.Wait()
	}
	if m.queue.Len() == 0 {
		return nil
	}
	front := m.queue.Front()
	m.queue.Remove(front)
	return front.Value
}

func (m *unboundedMailbox) NumberOfMessages() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queue.Len()
}

func (m *unboundedMailbox) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()
	m.cond.Broadcast()
}

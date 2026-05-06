/*
 * mailbox_bridge.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"

	"github.com/sopranoworks/gekka/actor/mailbox"
)

// NewMailboxBridge wraps a mailbox.MailboxFactory in the legacy
// actor.MailboxFactory interface so the existing SpawnActor mailbox-injection
// path can drive a Phase-1 mailbox without rewriting the dispatch loop.
//
// Phase 1 sub-commit 1.6 wires the new four factories
// (UnboundedMailbox/BoundedMailbox/{Unbounded,Bounded}ControlAwareMailbox)
// into actor construction. The actor cell in Start (base_actor.go) reads
// from BaseActor.Mailbox() — a chan any — so the bridge installs a drain
// goroutine that copies messages from the new mailbox.Mailbox.Dequeue() into
// that channel. Enqueue is bridged via mbSend; Close is bridged via
// mbClose.
//
// The bridge preserves the actor cell's two-channel system-vs-user priority
// (sub-commit 1.2): system messages still travel via SendSystem ↦
// systemMailbox, untouched. The drain goroutine only feeds the user side.
//
// Lifecycle:
//   - On install, NewMailbox is called with cfg; the resulting Mailbox is
//     held by the closure.
//   - mbSend → mb.Enqueue, returning false on any error so Tell semantics
//     match the legacy DropNewest behaviour.
//   - The drain goroutine runs until mb.Dequeue() returns nil (mailbox
//     closed and drained), then closes the user channel so Start exits.
//   - mbClose → close the done channel and call mb.Close, ensuring both
//     blocked senders and the drain goroutine wake up cleanly.
func NewMailboxBridge(factory mailbox.MailboxFactory, cfg mailbox.Config) MailboxFactory {
	return &mailboxBridge{factory: factory, cfg: cfg}
}

type mailboxBridge struct {
	factory mailbox.MailboxFactory
	cfg     mailbox.Config
}

func (mb *mailboxBridge) installInto(b *BaseActor) {
	inner := mb.factory.NewMailbox(mb.cfg)
	out := make(chan any)
	done := make(chan struct{})
	var closeOnce sync.Once

	b.mailbox = out

	b.mbSend = func(msg any) bool {
		select {
		case <-done:
			return false
		default:
		}
		// inner.Enqueue handles closed/full/nil internally and returns
		// the appropriate sentinel error. Any error means the message
		// was rejected, which the caller maps to DropNewest semantics.
		return inner.Enqueue(msg) == nil
	}

	b.mbClose = func() {
		closeOnce.Do(func() {
			close(done)
			inner.Close()
		})
	}

	go func() {
		defer close(out)
		for {
			msg := inner.Dequeue()
			if msg == nil {
				// Mailbox closed and fully drained.
				return
			}
			select {
			case out <- msg:
			case <-done:
				return
			}
		}
	}()
}

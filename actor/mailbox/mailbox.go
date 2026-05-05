/*
 * mailbox.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package mailbox holds the per-actor message queue used by the actor cell.
//
// This package is the receiver-side of Pekko/Akka's mailbox subsystem and is
// the foundation for control-aware (system vs user) message priority
// (perfect-pekko-compat roadmap, Phase 1).
//
// Sub-commit 1.1 ships the interface plus the default UnboundedMailbox
// implementation. The actor cell still uses its legacy single-channel mailbox
// in this commit; the cell-level swap to use Mailbox lands in sub-commit 1.2.
//
// Resolving HOCON to a factory:
//
//	id := "akka.dispatch.UnboundedMailbox" // from pekko.actor.default-mailbox.mailbox-type
//	factory := mailbox.Resolve(id)
//	mb := factory.NewMailbox(mailbox.Config{Capacity: 1024})
package mailbox

import (
	"errors"
	"sync"
	"time"
)

// Mailbox is the FIFO queue between the actor cell and the actor's user
// message loop. Implementations may use 1 or 2 internal channels; callers
// never see the difference.
//
// Contract:
//
//   - Enqueue rejects nil and returns ErrMailboxClosed once the mailbox has
//     been closed.
//   - Dequeue blocks until a message is available. It returns nil only when
//     the mailbox is closed and fully drained.
//   - NumberOfMessages is a best-effort snapshot — under concurrent
//     producers it is racy by design (mirrors Pekko's
//     MessageQueue.numberOfMessages).
//   - Close is idempotent and unblocks every caller blocked in Dequeue.
type Mailbox interface {
	Enqueue(msg any) error
	Dequeue() any
	NumberOfMessages() int
	Close()
}

// MailboxFactory builds a fresh Mailbox configured per HOCON. Each factory
// type is registered once at init and shared across actors; the factory is
// a stateless dispatcher that allocates a new Mailbox per call.
type MailboxFactory interface {
	NewMailbox(cfg Config) Mailbox
}

// Config is the resolved per-actor mailbox configuration.
//
// Bounded variants honour Capacity and PushTimeout; unbounded variants
// ignore both. A zero Capacity means unbounded; a zero PushTimeout means
// the sender blocks until space is available.
type Config struct {
	Capacity    int
	PushTimeout time.Duration
}

// ErrMailboxClosed is returned by Enqueue when the mailbox has been closed.
var ErrMailboxClosed = errors.New("mailbox: closed")

// ErrMailboxFull is returned by Enqueue on bounded mailboxes when capacity
// is exceeded and the push-timeout has elapsed.
var ErrMailboxFull = errors.New("mailbox: capacity exceeded")

// ErrNilMessage is returned by Enqueue when the caller passes a nil
// payload. Nil collides with Dequeue's closed-and-drained sentinel and is
// rejected at the boundary.
var ErrNilMessage = errors.New("mailbox: cannot enqueue nil message")

// ── Registry ────────────────────────────────────────────────────────────────
//
// The registry maps HOCON identifiers to factories. Each factory registers
// itself under all of its known identifiers in init():
//
//   - Short HOCON id: "unbounded", "bounded", "unbounded-control-aware",
//     "bounded-control-aware"
//   - Pekko FQCN: "org.apache.pekko.dispatch.{Unbounded,Bounded}Mailbox" and
//     "...{Unbounded,Bounded}ControlAwareMailbox"
//   - Akka FQCN: "akka.dispatch.{Unbounded,Bounded}Mailbox" and
//     "akka.dispatch.{Unbounded,Bounded}ControlAwareMailbox"

// DefaultMailboxID is the fallback identifier used when an actor has no
// explicit mailbox-type binding.
const DefaultMailboxID = "unbounded"

var (
	registryMu sync.RWMutex
	registry   = map[string]MailboxFactory{}
)

// Register associates factory with each of the given identifiers.
// Re-registering an existing id replaces the prior factory; this matches
// Pekko's last-write-wins config-merge semantics.
func Register(factory MailboxFactory, ids ...string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, id := range ids {
		registry[id] = factory
	}
}

// Lookup returns the factory registered under id, or nil if none.
func Lookup(id string) MailboxFactory {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return registry[id]
}

// Default returns the factory bound to DefaultMailboxID. It panics if no
// default has been registered, which indicates the package's init() did not
// run (e.g. the unbounded.go file was excluded from the build).
func Default() MailboxFactory {
	if f := Lookup(DefaultMailboxID); f != nil {
		return f
	}
	panic("mailbox: default factory not registered (import actor/mailbox)")
}

// Resolve returns the factory for id. An empty id resolves to Default. An
// unknown id returns nil so the caller can decide whether to fall back to
// the default or treat it as a fatal config error (Pekko semantics: fatal).
func Resolve(id string) MailboxFactory {
	if id == "" {
		return Default()
	}
	return Lookup(id)
}

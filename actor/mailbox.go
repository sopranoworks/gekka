/*
 * mailbox.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"container/heap"
	"sync"
)

// DropStrategy controls what BoundedMailbox does when it is at capacity.
type DropStrategy int

const (
	// DropNewest discards the incoming message when the mailbox is full.
	// This matches the default behavior of the existing channel-based mailbox.
	DropNewest DropStrategy = iota

	// DropOldest removes the oldest queued message to make room for the new one.
	DropOldest

	// BackPressure blocks the sender goroutine until space becomes available.
	BackPressure
)

// MailboxFactory creates a custom mailbox for an actor.
// Set Props.Mailbox to override the default unbounded channel mailbox.
//
//	ref, err := system.ActorOf(actor.Props{
//	    New:     func() actor.Actor { return &MyActor{BaseActor: actor.NewBaseActor()} },
//	    Mailbox: actor.NewBoundedMailbox(100, actor.DropOldest),
//	}, "worker")
type MailboxFactory interface {
	// installInto configures the actor's BaseActor with a custom mailbox.
	// Called once by InjectMailbox before the actor's goroutine starts.
	installInto(b *BaseActor)
}

// MessageSender is an optional interface for actors with custom mailbox semantics.
// When Tell detects this interface it calls Send instead of writing directly to
// the actor's channel. This enables back-pressure and DropOldest strategies.
type MessageSender interface {
	Send(msg any) bool
}

// MailboxCloser is an optional interface for actors with custom mailboxes that
// need special shutdown logic (e.g. a priority mailbox's drain goroutine).
// The actor system calls CloseMailbox when stopping an actor; if not implemented
// the actor system falls back to close(actor.Mailbox()).
type MailboxCloser interface {
	CloseMailbox()
}

// NewBoundedMailbox returns a MailboxFactory for a capacity-limited mailbox.
//
//   - capacity: maximum number of messages buffered before the drop strategy kicks in.
//   - drop: DropNewest, DropOldest, or BackPressure.
func NewBoundedMailbox(capacity int, drop DropStrategy) MailboxFactory {
	return &boundedFactory{cap: capacity, drop: drop}
}

// NewPriorityMailbox returns a MailboxFactory for an unbounded priority-queue mailbox.
// Messages are delivered to Receive in priority order rather than FIFO.
//
// less(a, b) should return true when message a has higher priority than b
// (i.e. should be processed before b). The heap is min-heap based, so the
// message for which less returns true against all others is processed first.
func NewPriorityMailbox(less func(a, b any) bool) MailboxFactory {
	return &priorityFactory{less: less}
}

// InjectMailbox applies a MailboxFactory to an actor that embeds BaseActor.
// It must be called before actor.Start(a). Called automatically by ActorOf /
// SpawnActor when Props.Mailbox is non-nil.
func InjectMailbox(a Actor, mf MailboxFactory) {
	type baseGetter interface{ baseActor() *BaseActor }
	if bg, ok := any(a).(baseGetter); ok {
		mf.installInto(bg.baseActor())
	}
}

// ── Mailbox Registry ─────────────────────────────────────────────────────────
//
// The registry maps FQCN strings (as used in Pekko/Akka HOCON config) to
// MailboxFactory constructors. This enables HOCON `mailbox-type` config keys
// to resolve to the correct Go implementation at actor spawn time.

var (
	mailboxRegistryMu sync.RWMutex
	mailboxRegistry   = map[string]MailboxFactory{}
)

// RegisterMailboxType registers a MailboxFactory under one or more FQCN keys.
// Call this from init() in mailbox implementation files.
//
//	actor.RegisterMailboxType(NewControlAwareMailbox(),
//	    "org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
//	    "akka.dispatch.UnboundedControlAwareMailbox",
//	)
func RegisterMailboxType(factory MailboxFactory, fqcns ...string) {
	mailboxRegistryMu.Lock()
	defer mailboxRegistryMu.Unlock()
	for _, fqcn := range fqcns {
		mailboxRegistry[fqcn] = factory
	}
}

// LookupMailboxType returns the MailboxFactory registered under fqcn, or nil
// if no such type has been registered.
func LookupMailboxType(fqcn string) MailboxFactory {
	mailboxRegistryMu.RLock()
	defer mailboxRegistryMu.RUnlock()
	return mailboxRegistry[fqcn]
}

// ── BoundedMailbox ────────────────────────────────────────────────────────────

type boundedFactory struct {
	cap  int
	drop DropStrategy
}

func (f *boundedFactory) installInto(b *BaseActor) {
	ch := make(chan any, f.cap)
	b.mailbox = ch
	switch f.drop {
	case BackPressure:
		b.mbSend = func(msg any) bool {
			ch <- msg // blocks until space is available
			return true
		}
	case DropOldest:
		b.mbSend = func(msg any) bool {
			for {
				select {
				case ch <- msg:
					return true
				default:
					// drain one message to make room
					select {
					case <-ch:
					default:
					}
				}
			}
		}
	default: // DropNewest
		b.mbSend = func(msg any) bool {
			select {
			case ch <- msg:
				return true
			default:
				return false
			}
		}
	}
	// CloseMailbox for bounded: just close the channel (same as default).
	// b.mbClose remains nil, so BaseActor.CloseMailbox() closes b.mailbox.
}

// ── PriorityMailbox ──────────────────────────────────────────────────────────

type priorityFactory struct {
	less func(a, b any) bool
}

func (f *priorityFactory) installInto(b *BaseActor) {
	out := make(chan any)     // Start() reads from this via Mailbox()
	done := make(chan struct{}) // closed by CloseMailbox to stop the drain goroutine
	signal := make(chan struct{}, 1)

	h := &msgPriorityHeap{less: f.less}
	heap.Init(h)
	var mu sync.Mutex

	b.mailbox = out

	b.mbSend = func(msg any) bool {
		mu.Lock()
		heap.Push(h, msg)
		mu.Unlock()
		// Notify drain goroutine; non-blocking — one pending signal is enough.
		select {
		case signal <- struct{}{}:
		default:
		}
		return true
	}

	b.mbClose = func() { close(done) }

	// Drain goroutine: delivers heap items to Start()'s range loop via out.
	//
	// Design: we track a "current" item — the one we intend to send next.
	// When blocked writing current to out, if a new signal arrives we peek at
	// the heap top: if it is higher-priority than current, we swap (push current
	// back, pop the higher-priority one). This ensures that the actor always
	// receives the highest-priority item available when it is ready to consume.
	go func() {
		defer close(out)
		var current any
		hasCurrent := false

		for {
			if !hasCurrent {
				// Nothing to send — wait for a new item.
				select {
				case <-done:
					return
				case <-signal:
					mu.Lock()
					if h.Len() > 0 {
						current = heap.Pop(h)
						hasCurrent = true
					}
					mu.Unlock()
				}
				continue
			}

			// We have an item; try to deliver it.
			select {
			case <-done:
				return
			case out <- current:
				// Delivered. Immediately pick the next highest-priority item.
				hasCurrent = false
				mu.Lock()
				if h.Len() > 0 {
					current = heap.Pop(h)
					hasCurrent = true
				}
				mu.Unlock()
			case <-signal:
				// A new item arrived while we were blocked writing current.
				// If it has higher priority, swap so the actor gets the best item.
				mu.Lock()
				if h.Len() > 0 && h.less(h.items[0], current) {
					heap.Push(h, current)
					current = heap.Pop(h)
				}
				mu.Unlock()
			}
		}
	}()
}

// ── msgPriorityHeap — container/heap adapter ──────────────────────────────────

type msgPriorityHeap struct {
	items []any
	less  func(a, b any) bool
}

func (h *msgPriorityHeap) Len() int           { return len(h.items) }
func (h *msgPriorityHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h *msgPriorityHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *msgPriorityHeap) Push(x any) {
	h.items = append(h.items, x)
}

func (h *msgPriorityHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.items = old[:n-1]
	return x
}

/*
 * mailbox_control.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "sync"

// NewControlAwareMailbox returns a MailboxFactory for an unbounded mailbox that
// prioritises ControlMessage instances over normal messages. Within each
// priority class FIFO order is preserved. Both queues are unbounded.
//
//	ref, err := system.ActorOf(actor.Props{
//	    New:     func() actor.Actor { return &MyActor{BaseActor: actor.NewBaseActor()} },
//	    Mailbox: actor.NewControlAwareMailbox(),
//	}, "ctrl-worker")
func NewControlAwareMailbox() MailboxFactory {
	return &controlAwareFactory{}
}

type controlAwareFactory struct{}

func (f *controlAwareFactory) installInto(b *BaseActor) {
	out := make(chan any)      // Start() reads from this via Mailbox()
	done := make(chan struct{}) // closed by CloseMailbox to stop the drain goroutine
	signal := make(chan struct{}, 1)

	var mu sync.Mutex
	var controlQ []any
	var normalQ []any

	b.mailbox = out

	b.mbSend = func(msg any) bool {
		mu.Lock()
		if _, ok := msg.(ControlMessage); ok {
			controlQ = append(controlQ, msg)
		} else {
			normalQ = append(normalQ, msg)
		}
		mu.Unlock()
		// Notify drain goroutine; non-blocking — one pending signal is enough.
		select {
		case signal <- struct{}{}:
		default:
		}
		return true
	}

	b.mbClose = func() { close(done) }

	// Drain goroutine: delivers control messages before normal messages to
	// Start()'s range loop via out.
	go func() {
		defer close(out)
		var current any
		hasCurrent := false

		// pop returns the next message respecting control-priority ordering.
		// Must be called with mu held.
		pop := func() (any, bool) {
			if len(controlQ) > 0 {
				msg := controlQ[0]
				controlQ = controlQ[1:]
				return msg, true
			}
			if len(normalQ) > 0 {
				msg := normalQ[0]
				normalQ = normalQ[1:]
				return msg, true
			}
			return nil, false
		}

		for {
			if !hasCurrent {
				// Nothing to send — wait for a new item.
				select {
				case <-done:
					return
				case <-signal:
					mu.Lock()
					current, hasCurrent = pop()
					mu.Unlock()
				}
				continue
			}

			// We have an item; try to deliver it.
			select {
			case <-done:
				return
			case out <- current:
				// Delivered. Immediately pick the next item.
				hasCurrent = false
				mu.Lock()
				current, hasCurrent = pop()
				mu.Unlock()
			case <-signal:
				// A new item arrived while we were blocked writing current.
				// If current is a normal message but a control message is now
				// available, swap so the actor gets the control message first.
				if _, isCtrl := current.(ControlMessage); !isCtrl {
					mu.Lock()
					if len(controlQ) > 0 {
						// Push current back to the front of normalQ and pop control.
						normalQ = append([]any{current}, normalQ...)
						current = controlQ[0]
						controlQ = controlQ[1:]
					}
					mu.Unlock()
				}
			}
		}
	}()
}

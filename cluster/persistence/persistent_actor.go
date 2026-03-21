/*
 * persistent_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"fmt"

	"github.com/sopranoworks/gekka/actor"
)

// PersistentActor is implemented by classic (non-generic) actors that require
// event-sourced persistence.
//
// Lifecycle:
//  1. The runtime (PersistentActorWrapper) enters Recovering state in PreStart.
//  2. All events stored under PersistenceId() are replayed by calling OnEvent.
//  3. Once replay is complete the actor enters Processing state; commands that
//     arrived during recovery are delivered in order.
//  4. For each new command the runtime calls OnCommand, which may call
//     ctx.Persist to durably record events and update internal state.
type PersistentActor interface {
	// PersistenceId returns the unique identifier used to store and retrieve
	// this actor's event log.  Must be stable across restarts.
	PersistenceId() string

	// OnCommand is called for every message received after recovery completes.
	// Validate the command and call ctx.Persist to record one or more events.
	// The call to ctx.Persist is synchronous: OnEvent is invoked before Persist
	// returns so the actor's state is always up to date when OnCommand returns.
	OnCommand(ctx PersistContext, cmd any)

	// OnEvent applies a single event to the actor's internal state.
	// Called both during recovery (replay) and after a successful Persist in
	// normal operation.  Must be deterministic and side-effect-free.
	OnEvent(event any)
}

// PersistContext is provided to OnCommand and gives access to the runtime's
// Persist function as well as the current actor references.
type PersistContext interface {
	// Persist durably records event, calls OnEvent(event) to update state, and
	// then invokes the optional handler with the same event.  If the journal
	// write fails the actor is stopped and handler is not called.
	Persist(event any, handler func(any))

	// Self returns the actor's own Ref.
	Self() actor.Ref

	// Sender returns the Ref of the message sender (nil if no sender).
	Sender() actor.Ref
}

// ── PersistentActorWrapper ────────────────────────────────────────────────────

// PersistentActorWrapper bridges a PersistentActor and the gekka actor runtime.
// Spawn it via actor.Props:
//
//	sys.ActorOf(actor.Props{
//	    New: func() actor.Actor {
//	        return NewPersistentActorWrapper(myActor, journal)
//	    },
//	}, "myActor")
type PersistentActorWrapper struct {
	actor.BaseActor
	inner      PersistentActor
	journal    Journal
	seqNr      int64
	recovering bool
	stash      []stashedEnvelope
}

type stashedEnvelope struct {
	msg    any
	sender actor.Ref
}

// NewPersistentActorWrapper returns an actor.Actor that drives inner through
// the event-sourcing lifecycle using journal for persistence.
func NewPersistentActorWrapper(inner PersistentActor, journal Journal) actor.Actor {
	return &PersistentActorWrapper{
		BaseActor:  actor.NewBaseActor(),
		inner:      inner,
		journal:    journal,
		recovering: true,
	}
}

// PreStart enters Recovering state and replays all persisted events, calling
// OnEvent for each.  Any commands that arrive during recovery are stashed and
// delivered once replay finishes.
func (w *PersistentActorWrapper) PreStart() {
	w.recovering = true
	if err := w.recover(); err != nil {
		w.Log().Error("PersistentActorWrapper: recovery failed — stopping actor",
			"persistenceId", w.inner.PersistenceId(), "error", err)
		if s, ok := w.System().(interface{ Stop(actor.Ref) }); ok {
			s.Stop(w.Self())
		}
		return
	}
	w.recovering = false
	w.Log().Info("PersistentActorWrapper: recovery complete",
		"persistenceId", w.inner.PersistenceId(), "seqNr", w.seqNr)

	// Deliver stashed messages in arrival order.
	stash := w.stash
	w.stash = nil
	for _, env := range stash {
		w.dispatchCommand(env.msg, env.sender)
	}
}

// Receive stashes messages during recovery; otherwise dispatches to OnCommand.
func (w *PersistentActorWrapper) Receive(msg any) {
	if w.recovering {
		w.stash = append(w.stash, stashedEnvelope{msg: msg, sender: w.Sender()})
		return
	}
	w.dispatchCommand(msg, w.Sender())
}

// recover replays the journal, calling OnEvent for each event.
func (w *PersistentActorWrapper) recover() error {
	stream, err := w.journal.Read(w.inner.PersistenceId(), 1)
	if err != nil {
		return fmt.Errorf("journal.Read: %w", err)
	}
	defer stream.Close()

	for {
		ev, ok := stream.Next()
		if !ok {
			break
		}
		w.inner.OnEvent(ev.Payload)
		w.seqNr = ev.SeqNr
	}
	return nil
}

// dispatchCommand calls OnCommand with a concrete PersistContext.
func (w *PersistentActorWrapper) dispatchCommand(msg any, sender actor.Ref) {
	ctx := &persistContextImpl{wrapper: w, sender: sender}
	w.inner.OnCommand(ctx, msg)
}

// ── persistContextImpl ────────────────────────────────────────────────────────

type persistContextImpl struct {
	wrapper *PersistentActorWrapper
	sender  actor.Ref
}

// Persist writes event to the journal, applies it via OnEvent, then calls
// handler (if non-nil) with the event.  Stops the actor on journal failure.
func (c *persistContextImpl) Persist(event any, handler func(any)) {
	c.wrapper.seqNr++
	if err := c.wrapper.journal.Write(c.wrapper.inner.PersistenceId(), c.wrapper.seqNr, event); err != nil {
		c.wrapper.Log().Error("PersistContext.Persist: journal write failed — stopping actor",
			"persistenceId", c.wrapper.inner.PersistenceId(),
			"seqNr", c.wrapper.seqNr,
			"error", err)
		c.wrapper.seqNr-- // roll back the counter so it stays consistent
		if s, ok := c.wrapper.System().(interface{ Stop(actor.Ref) }); ok {
			s.Stop(c.wrapper.Self())
		}
		return
	}
	c.wrapper.inner.OnEvent(event)
	if handler != nil {
		handler(event)
	}
}

func (c *persistContextImpl) Self() actor.Ref   { return c.wrapper.Self() }
func (c *persistContextImpl) Sender() actor.Ref { return c.sender }

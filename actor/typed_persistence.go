/*
 * typed_persistence.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"log/slog"
	"time"

	"github.com/sopranoworks/gekka/persistence"
)

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] struct {
	PersistenceID  string
	Journal        persistence.Journal
	SnapshotStore  persistence.SnapshotStore
	InitialState   State
	CommandHandler func(TypedContext[Command], State, Command) Effect[Event, State]
	EventHandler   func(State, Event) State

	// SnapshotSelectionCriteria for recovery
	SnapshotCriteria persistence.SnapshotSelectionCriteria
	// SnapshotInterval: if > 0, save a snapshot every N events
	SnapshotInterval uint64
}

// Effect represents the result of processing a command.
type Effect[Event any, State any] interface {
	apply(p internalPersistentActor[Event, State])
}

type internalPersistentActor[Event any, State any] interface {
	persist(events []Event, then func(State))
}

type persistEffect[Event any, State any] struct {
	events []Event
	then   func(State)
}

func (e *persistEffect[Event, State]) apply(p internalPersistentActor[Event, State]) {
	p.persist(e.events, e.then)
}

// Persist creates an effect that persists one or more events.
func Persist[Event any, State any](events ...Event) Effect[Event, State] {
	return &persistEffect[Event, State]{events: events}
}

// PersistThen creates an effect that persists events and then runs a callback.
func PersistThen[Event any, State any](then func(State), events ...Event) Effect[Event, State] {
	return &persistEffect[Event, State]{events: events, then: then}
}

type noneEffect[Event any, State any] struct{}

func (e *noneEffect[Event, State]) apply(p internalPersistentActor[Event, State]) {}

// None represents an effect that does nothing.
func None[Event any, State any]() Effect[Event, State] {
	return &noneEffect[Event, State]{}
}

// persistentActor is the internal implementation of a persistent actor.
type persistentActor[Command any, Event any, State any] struct {
	BaseActor
	behavior      *EventSourcedBehavior[Command, Event, State]
	state         State
	seqNr         uint64
	lastSnapSeqNr uint64
	recovering    bool
	tctx          TypedContext[Command]
	stash         []Command
}

func NewPersistentActor[Command any, Event any, State any](b *EventSourcedBehavior[Command, Event, State]) Actor {
	p := &persistentActor[Command, Event, State]{
		BaseActor:  NewBaseActor(),
		behavior:   b,
		state:      b.InitialState,
		recovering: true,
	}
	// We use a custom typedContext implementation that works with any Actor
	p.tctx = &persistentTypedContext[Command, Event, State]{actor: p}
	return p
}

type persistentTypedContext[C any, E any, S any] struct {
	actor *persistentActor[C, E, S]
}

func (c *persistentTypedContext[C, E, S]) Self() TypedActorRef[C] {
	return NewTypedActorRef[C](c.actor.Self())
}

func (c *persistentTypedContext[C, E, S]) System() ActorContext {
	return c.actor.System()
}

func (c *persistentTypedContext[C, E, S]) Log() *slog.Logger {
	return c.actor.Log().logger()
}

func (c *persistentTypedContext[C, E, S]) Watch(target Ref) {
	c.actor.System().Watch(c.actor.Self(), target)
}

func (c *persistentTypedContext[C, E, S]) Unwatch(target Ref) {
	if sys, ok := c.actor.System().(interface {
		Unwatch(watcher Ref, target Ref)
	}); ok {
		sys.Unwatch(c.actor.Self(), target)
	}
}

func (c *persistentTypedContext[C, E, S]) Stop(target Ref) {
	if sys, ok := c.actor.System().(interface {
		Stop(target Ref)
	}); ok {
		sys.Stop(target)
	}
}

func (p *persistentActor[Command, Event, State]) PreStart() {
	p.recover()
}

func (p *persistentActor[Command, Event, State]) Receive(msg any) {
	if p.recovering {
		if cmd, ok := msg.(Command); ok {
			p.stash = append(p.stash, cmd)
		}
		return
	}

	switch m := msg.(type) {
	case Command:
		effect := p.behavior.CommandHandler(p.tctx, p.state, m)
		if effect != nil {
			effect.apply(p)
		}
	case TerminatedMessage:
		// Handle termination
	}
}

func (p *persistentActor[Command, Event, State]) recover() {
	ctx := context.Background()

	// 1. Load snapshot
	if p.behavior.SnapshotStore != nil {
		snap, err := p.behavior.SnapshotStore.LoadSnapshot(ctx, p.behavior.PersistenceID, p.behavior.SnapshotCriteria)
		if err == nil && snap != nil {
			if s, ok := snap.Snapshot.(State); ok {
				p.state = s
				p.seqNr = snap.Metadata.SequenceNr
				p.lastSnapSeqNr = p.seqNr
			}
		}
	}

	// 2. Replay events
	if p.behavior.Journal != nil {
		err := p.behavior.Journal.ReplayMessages(ctx, p.behavior.PersistenceID, p.seqNr+1, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
			if event, ok := repr.Payload.(Event); ok {
				p.state = p.behavior.EventHandler(p.state, event)
				p.seqNr = repr.SequenceNr
			}
		})
		if err != nil {
			p.Log().Error("Recovery failed", "error", err)
			if s, ok := p.System().(interface{ Stop(Ref) }); ok {
				s.Stop(p.Self())
			}
			return
		}
		if p.lastSnapSeqNr == 0 {
			p.lastSnapSeqNr = p.seqNr
		}
	}

	p.recovering = false
	p.Log().Info("Recovery completed", "seqNr", p.seqNr)

	// 3. Process stashed commands
	for _, cmd := range p.stash {
		p.Receive(cmd)
	}
	p.stash = nil
}

func (p *persistentActor[Command, Event, State]) persist(events []Event, then func(State)) {
	ctx := context.Background()
	var reprs []persistence.PersistentRepr

	for _, event := range events {
		p.seqNr++
		reprs = append(reprs, persistence.PersistentRepr{
			PersistenceID: p.behavior.PersistenceID,
			SequenceNr:    p.seqNr,
			Payload:       event,
		})
	}

	err := p.behavior.Journal.AsyncWriteMessages(ctx, reprs)
	if err != nil {
		p.Log().Error("Failed to persist events", "error", err)
		if s, ok := p.System().(interface{ Stop(Ref) }); ok {
			s.Stop(p.Self())
		}
		return
	}

	// Apply events to state
	for _, event := range events {
		p.state = p.behavior.EventHandler(p.state, event)
	}

	// Check if snapshot is needed
	if p.behavior.SnapshotStore != nil && p.behavior.SnapshotInterval > 0 &&
		p.seqNr%p.behavior.SnapshotInterval == 0 {
		err := p.behavior.SnapshotStore.SaveSnapshot(ctx, persistence.SnapshotMetadata{
			PersistenceID: p.behavior.PersistenceID,
			SequenceNr:    p.seqNr,
			Timestamp:     time.Now().Unix(),
		}, p.state)
		if err == nil {
			p.lastSnapSeqNr = p.seqNr
		}
	}

	if then != nil {
		then(p.state)
	}
}

// SpawnPersistent creates a new persistent actor.
func SpawnPersistent[Command any, Event any, State any](ctx ActorContext, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...Props) (TypedActorRef[Command], error) {
	p := Props{
		New: func() Actor { return NewPersistentActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := ctx.ActorOf(p, name)
	if err != nil {
		return TypedActorRef[Command]{}, err
	}
	return NewTypedActorRef[Command](ref), nil
}

/*
 * typed_persistence.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
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

// WithPersistenceID returns a copy of the behavior with a new persistence ID.
func (b *EventSourcedBehavior[Command, Event, State]) WithPersistenceID(id string) *EventSourcedBehavior[Command, Event, State] {
	newB := *b
	newB.PersistenceID = id
	return &newB
}

// Effect represents the result of processing a command.
// It is a sealed type — only Persist, PersistThen, and None create values of this type.
type Effect[Event any, State any] interface {
	isEffect()
}

type persistEffect[Event any, State any] struct {
	events []Event
	then   func(State)
}

func (*persistEffect[Event, State]) isEffect() {}

// Persist creates an effect that persists one or more events.
func Persist[Event any, State any](events ...Event) Effect[Event, State] {
	return &persistEffect[Event, State]{events: events}
}

// PersistThen creates an effect that persists events and then runs a callback.
func PersistThen[Event any, State any](then func(State), events ...Event) Effect[Event, State] {
	return &persistEffect[Event, State]{events: events, then: then}
}

type noneEffect[Event any, State any] struct{}

func (*noneEffect[Event, State]) isEffect() {}

// None represents an effect that does nothing.
func None[Event any, State any]() Effect[Event, State] {
	return &noneEffect[Event, State]{}
}

// persistentActor is the internal implementation of a persistent actor.
type persistentActor[Command any, Event any, State any] struct {
	actor.BaseActor
	behavior      *EventSourcedBehavior[Command, Event, State]
	state         State
	seqNr         uint64
	lastSnapSeqNr uint64
	recovering    bool
	tctx          TypedContext[Command]
	stash         []Command // internal recovery stash; separate from user StashBuffer
	timers        *timerScheduler[Command]
	userStash     *stashBuffer[Command]
}

func NewPersistentActor[Command any, Event any, State any](b *EventSourcedBehavior[Command, Event, State]) actor.Actor {
	p := &persistentActor[Command, Event, State]{
		BaseActor:  actor.NewBaseActor(),
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

func (c *persistentTypedContext[C, E, S]) System() actor.ActorContext {
	return c.actor.System()
}

func (c *persistentTypedContext[C, E, S]) Log() *slog.Logger {
	return c.actor.Log().Logger()
}

func (c *persistentTypedContext[C, E, S]) Watch(target actor.Ref) {
	c.actor.System().Watch(c.actor.Self(), target)
}

func (c *persistentTypedContext[C, E, S]) Unwatch(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Unwatch(watcher actor.Ref, target actor.Ref)
	}); ok {
		sys.Unwatch(c.actor.Self(), target)
	}
}

func (c *persistentTypedContext[C, E, S]) Stop(target actor.Ref) {
	if sys, ok := c.actor.System().(interface {
		Stop(target actor.Ref)
	}); ok {
		sys.Stop(target)
	}
}

func (c *persistentTypedContext[C, E, S]) Passivate() {
	if parent := c.actor.Parent(); parent != nil {
		parent.Tell(actor.Passivate{Entity: c.actor.Self()}, c.actor.Self())
	}
}

func (c *persistentTypedContext[C, E, S]) Timers() TimerScheduler[C] {
	return c.actor.timers
}

func (c *persistentTypedContext[C, E, S]) Stash() StashBuffer[C] {
	return c.actor.userStash
}

func (c *persistentTypedContext[C, E, S]) Sender() actor.Ref {
	return c.actor.Sender()
}

func (c *persistentTypedContext[C, E, S]) Ask(target actor.Ref, msgFactory func(actor.Ref) any, transform func(any, error) C) {
	askID := askCounter.Add(1)
	timerKey := fmt.Sprintf("ask-timeout-%d", askID)
	timeout := 3 * time.Second // Default timeout

	completed := &atomic.Bool{}

	// 1. Create transformed message for timeout
	timeoutMsg := transform(nil, actor.ErrAskTimeout)

	// 2. Schedule timeout using existing TimerScheduler
	c.Timers().StartSingleTimer(timerKey, timeoutMsg, timeout)

	// 3. Create temporary responder
	responder := &contextAskResponder[C]{
		self:      c.Self(),
		transform: transform,
		timerKey:  timerKey,
		timers:    c.Timers(),
		completed: completed,
	}

	// 4. Send the message
	msg := msgFactory(responder)
	target.Tell(msg)
}

func (p *persistentActor[Command, Event, State]) PreStart() {
	p.timers = newTimerScheduler[Command](p.Self())
	p.userStash = newStashBuffer[Command](p.Self(), actor.DefaultStashCapacity)
	p.recover()
}

func (p *persistentActor[Command, Event, State]) PostStop() {
	p.timers.CancelAll()
}

func (p *persistentActor[Command, Event, State]) Receive(msg any) {
	p.Log().Debug("PersistentActor received message",
		"msgType", fmt.Sprintf("%T", msg),
		"recovering", p.recovering)

	if p.recovering {
		if cmd, ok := msg.(Command); ok {
			p.Log().Debug("Stashing command during recovery")
			p.stash = append(p.stash, cmd)
		}
		return
	}

	// Direct type assertion to Command
	if cmd, ok := msg.(Command); ok {
		p.Log().Debug("Handling persistent command")
		effect := p.behavior.CommandHandler(p.tctx, p.state, cmd)
		if pe, ok := effect.(*persistEffect[Event, State]); ok {
			p.persist(pe.events, pe.then)
		}
		// *noneEffect and nil are no-ops.
		return
	}

	switch msg.(type) {
	case actor.TerminatedMessage:
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
			if s, ok := p.System().(interface{ Stop(actor.Ref) }); ok {
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

	p.Log().Debug("Persisting events", "count", len(events))

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
		if s, ok := p.System().(interface{ Stop(actor.Ref) }); ok {
			s.Stop(p.Self())
		}
		return
	}

	// Apply events to state
	for _, event := range events {
		p.Log().Debug("Applying event to state", "seqNr", p.seqNr)
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
func SpawnPersistent[Command any, Event any, State any](ctx actor.ActorContext, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	p := actor.Props{
		New: func() actor.Actor { return NewPersistentActor(behavior) },
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

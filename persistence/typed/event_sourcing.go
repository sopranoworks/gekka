/*
 * event_sourcing.go
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
	"math"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
)

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] struct {
	PersistenceID  string
	Journal        persistence.Journal
	SnapshotStore  persistence.SnapshotStore
	InitialState   State
	CommandHandler func(typed.TypedContext[Command], State, Command) Effect[Event, State]
	EventHandler   func(State, Event) State

	// Tagger, if non-nil, is called for each event being persisted and returns the
	// set of tags that should be associated with that event.  The tags are stored
	// in the journal alongside the event and enable cross-actor queries via
	// EventsByTag.
	Tagger func(Event) []string

	// SnapshotSelectionCriteria for recovery
	SnapshotCriteria persistence.SnapshotSelectionCriteria
	// SnapshotInterval: if > 0, save a snapshot every N events
	SnapshotInterval uint64
	// MaxSnapshots: if > 0, retain only the latest N snapshot checkpoints and
	// delete older ones after each save.  Requires SnapshotInterval > 0.
	// Example: SnapshotInterval=1, MaxSnapshots=2 keeps the two most recent snapshots.
	MaxSnapshots int
	// SnapshotWhen, if non-nil, is evaluated after each event application.
	// A snapshot is saved whenever it returns true (OR'd with SnapshotInterval).
	SnapshotWhen func(state State, event Event, seqNr uint64) bool
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

// PersistEffectEvents returns the events from a persist effect, or nil if the
// effect is not a persist effect. This allows sub-packages (e.g. replicated)
// to extract events without accessing unexported types.
func PersistEffectEvents[Event any, State any](e Effect[Event, State]) (events []Event, then func(State), ok bool) {
	if pe, ok := e.(*persistEffect[Event, State]); ok {
		return pe.events, pe.then, true
	}
	return nil, nil, false
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
	tctx          typed.TypedContext[Command]
	stash         []Command // internal recovery stash; separate from user StashBuffer
	timers        *actor.TimerSchedulerImpl[Command]
	userStash     *actor.StashBufferImpl[Command]
}

func NewPersistentActor[Command any, Event any, State any](b *EventSourcedBehavior[Command, Event, State]) actor.Actor {
	p := &persistentActor[Command, Event, State]{
		BaseActor:  actor.NewBaseActor(),
		behavior:   b,
		state:      b.InitialState,
		recovering: true,
	}
	p.tctx = &persistentTypedContext[Command, Event, State]{actor: p}
	return p
}

type persistentTypedContext[C any, E any, S any] struct {
	actor *persistentActor[C, E, S]
}

func (c *persistentTypedContext[C, E, S]) Self() typed.TypedActorRef[C] {
	return typed.NewTypedActorRef[C](c.actor.Self())
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

func (c *persistentTypedContext[C, E, S]) Timers() typed.TimerScheduler[C] {
	return c.actor.timers
}

func (c *persistentTypedContext[C, E, S]) Stash() typed.StashBuffer[C] {
	return c.actor.userStash
}

func (c *persistentTypedContext[C, E, S]) Sender() actor.Ref {
	return c.actor.Sender()
}

var askCounter atomic.Uint64

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

func (c *persistentTypedContext[C, E, S]) Spawn(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().Spawn(behavior, name)
}

func (c *persistentTypedContext[C, E, S]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return c.actor.System().SpawnAnonymous(behavior)
}

func (c *persistentTypedContext[C, E, S]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().SystemActorOf(behavior, name)
}

type contextAskResponder[T any] struct {
	self      typed.TypedActorRef[T]
	transform func(any, error) T
	timerKey  string
	timers    typed.TimerScheduler[T]
	completed *atomic.Bool
}

func (r *contextAskResponder[T]) Tell(msg any, sender ...actor.Ref) {
	if r.completed.CompareAndSwap(false, true) {
		r.timers.Cancel(r.timerKey)
		transformed := r.transform(msg, nil)
		r.self.Tell(transformed)
	}
}

func (r *contextAskResponder[T]) Path() string {
	return "/temp/context-ask"
}

func (p *persistentActor[Command, Event, State]) PreStart() {
	p.timers = actor.NewTimerScheduler[Command](p.Self())
	p.userStash = actor.NewStashBuffer[Command](p.Self(), actor.DefaultStashCapacity)
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
		var tags []string
		if p.behavior.Tagger != nil {
			tags = p.behavior.Tagger(event)
		}
		reprs = append(reprs, persistence.PersistentRepr{
			PersistenceID: p.behavior.PersistenceID,
			SequenceNr:    p.seqNr,
			Payload:       event,
			Tags:          tags,
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

	// Apply events to state; evaluate snapshot predicates per event.
	for i, event := range events {
		eventSeqNr := reprs[i].SequenceNr
		p.Log().Debug("Applying event to state", "seqNr", eventSeqNr)
		p.state = p.behavior.EventHandler(p.state, event)

		// Determine whether a snapshot should be saved after this event (OR logic).
		shouldSnap := false
		if p.behavior.SnapshotInterval > 0 && eventSeqNr%p.behavior.SnapshotInterval == 0 {
			shouldSnap = true
		}
		if p.behavior.SnapshotWhen != nil && p.behavior.SnapshotWhen(p.state, event, eventSeqNr) {
			shouldSnap = true
		}

		if shouldSnap && p.behavior.SnapshotStore != nil {
			snapErr := p.behavior.SnapshotStore.SaveSnapshot(ctx, persistence.SnapshotMetadata{
				PersistenceID: p.behavior.PersistenceID,
				SequenceNr:    eventSeqNr,
				Timestamp:     time.Now().Unix(),
			}, p.state)
			if snapErr == nil {
				p.lastSnapSeqNr = eventSeqNr
				// Automated cleanup when using SnapshotInterval-based retention.
				if p.behavior.MaxSnapshots > 0 && p.behavior.SnapshotInterval > 0 {
					keepCount := uint64(p.behavior.MaxSnapshots) * p.behavior.SnapshotInterval
					if eventSeqNr > keepCount {
						deleteUpTo := eventSeqNr - keepCount
						if delErr := p.behavior.SnapshotStore.DeleteSnapshots(ctx, p.behavior.PersistenceID, persistence.SnapshotSelectionCriteria{
							MaxSequenceNr: deleteUpTo,
							MaxTimestamp:  math.MaxInt64,
						}); delErr != nil {
							p.Log().Error("snapshot cleanup failed", "error", delErr, "deleteUpTo", deleteUpTo)
						}
					}
				}
			}
		}
	}

	if then != nil {
		then(p.state)
	}
}

// SpawnPersistent creates a new persistent actor.
func SpawnPersistent[Command any, Event any, State any](ctx actor.ActorContext, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (typed.TypedActorRef[Command], error) {
	p := actor.Props{
		New: func() actor.Actor { return NewPersistentActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := ctx.ActorOf(p, name)
	if err != nil {
		return typed.TypedActorRef[Command]{}, err
	}
	return typed.NewTypedActorRef[Command](ref), nil
}

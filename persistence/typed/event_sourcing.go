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

	// RetentionCriteria, if set, governs automated snapshot retention and
	// event deletion.  It takes precedence over the SnapshotInterval and
	// MaxSnapshots fields above when its SnapshotEveryNEvents > 0.
	RetentionCriteria *RetentionCriteria

	// RecoveryStrategy controls how recovery proceeds.  Nil means normal
	// recovery (load snapshot + replay all events).
	RecoveryStrategy *RecoveryStrategy

	// SignalHandler, if non-nil, receives lifecycle signals such as
	// [RecoveryCompleted] after recovery finishes.
	SignalHandler func(signal PersistenceSignal)
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
	stash             []Command // internal recovery stash; separate from user StashBuffer
	timers            *actor.TimerSchedulerImpl[Command]
	userStash         *actor.StashBufferImpl[Command]
	userStashPending  []Command
	drainingUserStash bool
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
	p.userStash = actor.NewStashBuffer[Command](GetDefaultStashCapacity(), func(cmd Command) {
		p.userStashPending = append(p.userStashPending, cmd)
	})
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
			capacity := GetDefaultStashCapacity()
			if capacity > 0 && len(p.stash) >= capacity {
				switch GetDefaultStashOverflowStrategy() {
				case "fail":
					p.Log().Error("PersistentActor: recovery stash overflow — failing actor",
						"persistenceID", p.behavior.PersistenceID,
						"capacity", capacity)
					if s, ok := p.System().(interface{ Stop(actor.Ref) }); ok {
						s.Stop(p.Self())
					}
				default: // "drop"
					p.Log().Warn("PersistentActor: recovery stash full — dropping command",
						"persistenceID", p.behavior.PersistenceID,
						"capacity", capacity,
						"msgType", fmt.Sprintf("%T", cmd))
				}
				return
			}
			if GetLogStashing() {
				p.Log().Debug("Stashing command during recovery",
					"persistenceID", p.behavior.PersistenceID,
					"size", len(p.stash)+1,
					"msgType", fmt.Sprintf("%T", cmd))
			}
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
		p.drainUserStash()
		return
	}

	switch msg.(type) {
	case actor.TerminatedMessage:
		// Handle termination
	}
}

func (p *persistentActor[Command, Event, State]) drainUserStash() {
	if p.drainingUserStash {
		return
	}
	p.drainingUserStash = true
	defer func() { p.drainingUserStash = false }()

	for len(p.userStashPending) > 0 {
		next := p.userStashPending[0]
		p.userStashPending = p.userStashPending[1:]
		p.Receive(next)
	}
}

func (p *persistentActor[Command, Event, State]) recover() {
	ctx := context.Background()

	// Acquire a recovery slot from the global limiter to prevent
	// overwhelming the journal backend when many actors start at once.
	// Corresponds to pekko.persistence.max-concurrent-recoveries.
	rl, err := persistence.AcquireRecovery(ctx)
	if err != nil {
		p.Log().Error("Recovery limiter acquisition failed", "error", err)
		return
	}
	defer rl.Release()

	// Check for disabled recovery
	if p.behavior.RecoveryStrategy != nil && p.behavior.RecoveryStrategy.Disabled {
		// Skip recovery — start fresh but read highest seqNr to avoid conflicts
		if p.behavior.Journal != nil {
			highSeq, err := p.behavior.Journal.ReadHighestSequenceNr(ctx, p.behavior.PersistenceID, 0)
			if err == nil {
				p.seqNr = highSeq
			}
		}
		p.recovering = false
		p.Log().Info("Recovery disabled, starting fresh", "seqNr", p.seqNr)
		p.deliverRecoveryCompleted()
		p.processStash()
		return
	}

	// 1. Load snapshot
	if p.behavior.SnapshotStore != nil {
		criteria := p.behavior.SnapshotCriteria
		if criteria == (persistence.SnapshotSelectionCriteria{}) {
			criteria = persistence.LatestSnapshotCriteria()
		}
		if p.behavior.RecoveryStrategy != nil && p.behavior.RecoveryStrategy.SnapshotSelectionCriteria != nil {
			criteria = *p.behavior.RecoveryStrategy.SnapshotSelectionCriteria
		}
		snap, err := p.behavior.SnapshotStore.LoadSnapshot(ctx, p.behavior.PersistenceID, criteria)
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
		replayFilter := p.getReplayFilter()
		err := p.behavior.Journal.ReplayMessages(ctx, p.behavior.PersistenceID, p.seqNr+1, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
			if event, ok := repr.Payload.(Event); ok {
				if replayFilter != nil && !replayFilter(repr) {
					// Skip this event but advance seqNr
					p.seqNr = repr.SequenceNr
					return
				}
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
	p.deliverRecoveryCompleted()
	p.processStash()
}

func (p *persistentActor[Command, Event, State]) getReplayFilter() func(persistence.PersistentRepr) bool {
	if p.behavior.RecoveryStrategy != nil && p.behavior.RecoveryStrategy.ReplayFilter != nil {
		return p.behavior.RecoveryStrategy.ReplayFilter
	}
	return nil
}

func (p *persistentActor[Command, Event, State]) deliverRecoveryCompleted() {
	if p.behavior.SignalHandler != nil {
		p.behavior.SignalHandler(RecoveryCompleted{HighestSequenceNr: p.seqNr})
	}
	// pekko.persistence.typed.snapshot-on-recovery — save a snapshot once
	// recovery finishes so that the next restart only replays new events.
	if GetSnapshotOnRecovery() && p.behavior.SnapshotStore != nil && p.seqNr > 0 && p.seqNr != p.lastSnapSeqNr {
		ctx := context.Background()
		meta := persistence.SnapshotMetadata{
			PersistenceID: p.behavior.PersistenceID,
			SequenceNr:    p.seqNr,
			Timestamp:     time.Now().UnixNano(),
		}
		if err := p.behavior.SnapshotStore.SaveSnapshot(ctx, meta, p.state); err != nil {
			p.Log().Error("snapshot-on-recovery save failed",
				"persistenceID", p.behavior.PersistenceID,
				"error", err)
			return
		}
		p.lastSnapSeqNr = p.seqNr
	}
}

func (p *persistentActor[Command, Event, State]) processStash() {
	if GetLogStashing() && len(p.stash) > 0 {
		p.Log().Debug("Unstashing recovery commands",
			"persistenceID", p.behavior.PersistenceID,
			"count", len(p.stash))
	}
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
		if p.behavior.RetentionCriteria != nil && p.behavior.RetentionCriteria.SnapshotEveryNEvents > 0 &&
			eventSeqNr%p.behavior.RetentionCriteria.SnapshotEveryNEvents == 0 {
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
				p.applyRetentionCleanup(ctx, eventSeqNr)
			}
		}
	}

	if then != nil {
		then(p.state)
	}
}

func (p *persistentActor[Command, Event, State]) applyRetentionCleanup(ctx context.Context, eventSeqNr uint64) {
	// RetentionCriteria-based cleanup
	if rc := p.behavior.RetentionCriteria; rc != nil && rc.KeepNSnapshots > 0 {
		interval := rc.SnapshotEveryNEvents
		if interval == 0 {
			interval = 1
		}
		keepCount := uint64(rc.KeepNSnapshots) * interval
		if eventSeqNr > keepCount {
			deleteUpTo := eventSeqNr - keepCount
			if delErr := p.behavior.SnapshotStore.DeleteSnapshots(ctx, p.behavior.PersistenceID, persistence.SnapshotSelectionCriteria{
				MaxSequenceNr: deleteUpTo,
				MaxTimestamp:  math.MaxInt64,
			}); delErr != nil {
				p.Log().Error("snapshot cleanup failed", "error", delErr, "deleteUpTo", deleteUpTo)
			}
			// Delete old events if configured
			if rc.DeleteEventsOnSnapshot {
				if delErr := p.behavior.Journal.AsyncDeleteMessagesTo(ctx, p.behavior.PersistenceID, deleteUpTo); delErr != nil {
					p.Log().Error("event cleanup failed", "error", delErr, "deleteUpTo", deleteUpTo)
				}
			}
		}
		return
	}

	// Legacy MaxSnapshots-based cleanup
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

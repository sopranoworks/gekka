/*
 * fsm.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// FSMSnapshot is the on-disk representation of a PersistentFSM snapshot.
// Persisted by the snapshot-store and consumed during recovery to seed the
// FSM's state and data before journal replay continues from the post-snapshot
// sequence number.
type FSMSnapshot[S comparable, D any] struct {
	State S `json:"state"`
	Data  D `json:"data"`
}

// ── FSM event and result types ────────────────────────────────────────────────

// FSMEvent carries the incoming message and current data for a PersistentFSM
// state handler.
type FSMEvent[D any] struct {
	Msg  any
	Data D
}

// FSMStateResult is returned by a PersistentFSM state handler.
// Events holds the domain events that should be persisted (and then applied via
// ApplyEvent) before the FSM transitions to NextState with NextData.
type FSMStateResult[S comparable, D any, E any] struct {
	NextState S
	NextData  D
	Events    []E
	Stop      bool
	Unhandled bool
}

// ── FSMStateBuilder – fluent DSL ──────────────────────────────────────────────

// FSMStateBuilder helps construct an FSMStateResult using a fluent API that
// mirrors the classic BaseFSM DSL.
type FSMStateBuilder[S comparable, D any, E any] struct {
	result FSMStateResult[S, D, E]
}

// Using sets the next data value.
func (b *FSMStateBuilder[S, D, E]) Using(data D) *FSMStateBuilder[S, D, E] {
	b.result.NextData = data
	return b
}

// Persisting appends domain events that will be persisted before the transition.
func (b *FSMStateBuilder[S, D, E]) Persisting(events ...E) *FSMStateBuilder[S, D, E] {
	b.result.Events = append(b.result.Events, events...)
	return b
}

// Build finalises the builder and returns the FSMStateResult.
func (b *FSMStateBuilder[S, D, E]) Build() FSMStateResult[S, D, E] {
	return b.result
}

// ── PersistentFSM ─────────────────────────────────────────────────────────────

// PersistentFSM combines the Finite State Machine pattern with event-sourced
// persistence.  On each state transition the actor can persist one or more
// domain events (type E); those events are later replayed during recovery to
// reconstruct the FSM state (S) and data (D) without re-executing command
// handlers.
//
// Type parameters:
//
//	S – FSM state type; must be comparable (used as map key).
//	D – FSM data type.
//	E – persisted event type.
type PersistentFSM[S comparable, D any, E any] struct {
	actor.BaseActor

	persistenceID string
	journal       Journal
	seqNr         uint64
	recovering    bool
	stash         []any // commands received during recovery

	currentState S
	stateData    D

	// handlers is keyed by the FSM state value (S must be comparable).
	handlers         map[any]func(FSMEvent[D]) FSMStateResult[S, D, E]
	unhandledHandler func(FSMEvent[D]) FSMStateResult[S, D, E]

	// applyEvent reconstructs (S, D) from a persisted event E during recovery.
	applyEventFn func(S, D, E) (S, D)

	// onTransition is called after every state change (not during recovery).
	transitions []func(from S, to S)

	startCalled bool

	// async write support (PersistAsync / PersistAllAsync)
	asyncWriteCh chan asyncWriteTask
	asyncOnce    sync.Once

	// pekko.persistence.fsm.snapshot-after support.
	snapshotStore        SnapshotStore
	snapshotAfter        int // 0 = use package default
	eventsSinceSnapshot  int
}

// NewPersistentFSM creates a PersistentFSM that stores events under persistenceID
// using journal.
func NewPersistentFSM[S comparable, D any, E any](persistenceID string, journal Journal) *PersistentFSM[S, D, E] {
	return &PersistentFSM[S, D, E]{
		BaseActor:     actor.NewBaseActor(),
		persistenceID: persistenceID,
		journal:       journal,
		handlers:      make(map[any]func(FSMEvent[D]) FSMStateResult[S, D, E]),
	}
}

// ── DSL methods ───────────────────────────────────────────────────────────────

// StartWith sets the initial state and data.  Must be called before the actor
// is started (typically in the constructor or PreStart override).
func (f *PersistentFSM[S, D, E]) StartWith(state S, data D) {
	f.currentState = state
	f.stateData = data
	f.startCalled = true
}

// When registers a handler for the given FSM state.
func (f *PersistentFSM[S, D, E]) When(state S, handler func(FSMEvent[D]) FSMStateResult[S, D, E]) {
	f.handlers[state] = handler
}

// WhenUnhandled registers a fallback handler invoked when no state handler
// matches the current state or the matched handler returns Unhandled.
func (f *PersistentFSM[S, D, E]) WhenUnhandled(handler func(FSMEvent[D]) FSMStateResult[S, D, E]) {
	f.unhandledHandler = handler
}

// ApplyEvent registers the function used to fold a persisted event E into the
// current (state, data) pair during recovery.  This is the event handler for
// the read/recovery path — it must be a pure function.
func (f *PersistentFSM[S, D, E]) ApplyEvent(fn func(state S, data D, event E) (S, D)) {
	f.applyEventFn = fn
}

// OnTransition registers a callback that is invoked after every state change
// during normal operation (not during recovery).
func (f *PersistentFSM[S, D, E]) OnTransition(handler func(from S, to S)) {
	f.transitions = append(f.transitions, handler)
}

// WithSnapshotStore opts the FSM into snapshotting via store. When combined
// with a positive snapshot-after value (per-FSM via SetSnapshotAfter or the
// global default from pekko.persistence.fsm.snapshot-after), a snapshot of the
// (state, data) pair is saved every N persisted events and is consulted during
// recovery before journal replay.
func (f *PersistentFSM[S, D, E]) WithSnapshotStore(store SnapshotStore) *PersistentFSM[S, D, E] {
	f.snapshotStore = store
	return f
}

// SetSnapshotAfter overrides the package-default snapshot-after value for
// this FSM instance. n <= 0 reverts to the package default.
//
// HOCON: pekko.persistence.fsm.snapshot-after.
func (f *PersistentFSM[S, D, E]) SetSnapshotAfter(n int) *PersistentFSM[S, D, E] {
	if n < 0 {
		n = 0
	}
	f.snapshotAfter = n
	return f
}

// effectiveSnapshotAfter returns the per-FSM override when set, otherwise the
// package-default from auto_start.go.
func (f *PersistentFSM[S, D, E]) effectiveSnapshotAfter() int {
	if f.snapshotAfter > 0 {
		return f.snapshotAfter
	}
	return GetDefaultFSMSnapshotAfter()
}

// ── Builder helpers (mirrors BaseFSM) ─────────────────────────────────────────

// Goto returns a builder for a transition to nextState.
func (f *PersistentFSM[S, D, E]) Goto(nextState S) *FSMStateBuilder[S, D, E] {
	return &FSMStateBuilder[S, D, E]{result: FSMStateResult[S, D, E]{NextState: nextState, NextData: f.stateData}}
}

// Stay returns a builder that keeps the current state.
func (f *PersistentFSM[S, D, E]) Stay() *FSMStateBuilder[S, D, E] {
	return &FSMStateBuilder[S, D, E]{result: FSMStateResult[S, D, E]{NextState: f.currentState, NextData: f.stateData}}
}

// StopFSM returns a result that halts the FSM actor.
func (f *PersistentFSM[S, D, E]) StopFSM() FSMStateResult[S, D, E] {
	return FSMStateResult[S, D, E]{NextState: f.currentState, NextData: f.stateData, Stop: true}
}

// Unhandled returns a result indicating the message was not handled.
func (f *PersistentFSM[S, D, E]) Unhandled() FSMStateResult[S, D, E] {
	return FSMStateResult[S, D, E]{NextState: f.currentState, NextData: f.stateData, Unhandled: true}
}

// ── State accessors ───────────────────────────────────────────────────────────

// State returns the current FSM state.
func (f *PersistentFSM[S, D, E]) State() S { return f.currentState }

// Data returns the current FSM data.
func (f *PersistentFSM[S, D, E]) Data() D { return f.stateData }

// ── actor.Actor lifecycle ─────────────────────────────────────────────────────

// PreStart triggers recovery from the journal.  Concrete actors embedding
// PersistentFSM must call f.PersistentFSMPreStart() from their own PreStart if
// they override it.
func (f *PersistentFSM[S, D, E]) PreStart() {
	f.PersistentFSMPreStart()
}

// PersistentFSMPreStart is the reusable recovery entry point for embedders.
func (f *PersistentFSM[S, D, E]) PersistentFSMPreStart() {
	if !f.startCalled {
		panic("PersistentFSM: StartWith must be called before the actor starts")
	}
	if f.applyEventFn == nil {
		panic("PersistentFSM: ApplyEvent must be registered before the actor starts")
	}
	f.recovering = true
	f.recover()
}

// Receive dispatches messages: during recovery they are stashed; afterwards
// they are routed to the registered state handlers. Internal persistAsyncAck
// messages are intercepted here and never forwarded to state handlers.
func (f *PersistentFSM[S, D, E]) Receive(msg any) {
	// Intercept internal async-persist acknowledgements.
	if ack, ok := msg.(persistAsyncAck); ok {
		if ack.err != nil {
			f.Log().Error("PersistentFSM: async persist failed", "error", ack.err)
			return
		}
		for _, h := range ack.handlers {
			h()
		}
		return
	}
	if f.recovering {
		f.stash = append(f.stash, msg)
		return
	}
	f.dispatch(msg)
}

// PersistAsync enqueues event for asynchronous journal write and returns
// immediately so the actor can keep processing incoming commands. handler is
// invoked (on the actor's goroutine, via the mailbox) after the journal write
// succeeds. Multiple PersistAsync calls are written and acknowledged in the
// order they were issued.
//
// Unlike the synchronous persist path in FSMStateResult.Events, PersistAsync
// does not stash incoming commands: the actor remains responsive while the
// write is in flight.
func (f *PersistentFSM[S, D, E]) PersistAsync(event E, handler func(E)) {
	f.seqNr++
	repr := PersistentRepr{
		PersistenceID: f.persistenceID,
		SequenceNr:    f.seqNr,
		Payload:       event,
	}
	ev := event  // capture for closure
	fn := handler // capture for closure
	bound := func() { fn(ev) }
	f.startAsyncWriter()
	f.asyncWriteCh <- asyncWriteTask{
		reprs:    []PersistentRepr{repr},
		handlers: []func(){bound},
	}
}

// PersistAllAsync persists a batch of events asynchronously. handler is called
// once per event, in order, after the entire batch has been written to the
// journal. The actor continues processing commands during the write.
func (f *PersistentFSM[S, D, E]) PersistAllAsync(events []E, handler func(E)) {
	if len(events) == 0 {
		return
	}
	reprs := make([]PersistentRepr, len(events))
	handlers := make([]func(), len(events))
	for i, event := range events {
		f.seqNr++
		ev := event  // capture
		fn := handler // capture
		reprs[i] = PersistentRepr{
			PersistenceID: f.persistenceID,
			SequenceNr:    f.seqNr,
			Payload:       ev,
		}
		handlers[i] = func() { fn(ev) }
	}
	f.startAsyncWriter()
	f.asyncWriteCh <- asyncWriteTask{reprs: reprs, handlers: handlers}
}

// PostStop shuts down the background async-write goroutine, if started.
// Embedders that override PostStop must call f.PersistentFSM.PostStop().
func (f *PersistentFSM[S, D, E]) PostStop() {
	if f.asyncWriteCh != nil {
		close(f.asyncWriteCh)
	}
}

// startAsyncWriter initialises asyncWriteCh and starts the background write
// goroutine exactly once (on the first PersistAsync / PersistAllAsync call).
func (f *PersistentFSM[S, D, E]) startAsyncWriter() {
	f.asyncOnce.Do(func() {
		f.asyncWriteCh = make(chan asyncWriteTask, 1024)
		go asyncWriteLoop(f.journal, f.asyncWriteCh, f.Mailbox())
	})
}

// ── Internal ─────────────────────────────────────────────────────────────────

func (f *PersistentFSM[S, D, E]) recover() {
	ctx := context.Background()

	// Acquire a recovery slot from the global limiter.
	rl, err := AcquireRecovery(ctx)
	if err != nil {
		f.Log().Error("Recovery limiter acquisition failed", "persistenceID", f.persistenceID, "error", err)
		return
	}
	defer rl.Release()

	// Snapshot replay (pekko.persistence.fsm.snapshot-after).  When a snapshot
	// store is configured, load the highest available snapshot first so that
	// journal replay can resume from the post-snapshot sequence number.
	if f.snapshotStore != nil {
		crit := SnapshotSelectionCriteria{
			MaxSequenceNr: ^uint64(0),
			MaxTimestamp:  int64(^uint64(0) >> 1), // math.MaxInt64
		}
		if snap, snapErr := f.snapshotStore.LoadSnapshot(ctx, f.persistenceID, crit); snapErr == nil && snap != nil {
			if payload, ok := snap.Snapshot.(FSMSnapshot[S, D]); ok {
				f.currentState = payload.State
				f.stateData = payload.Data
				f.seqNr = snap.Metadata.SequenceNr
			}
		}
	}

	if f.journal != nil {
		err := f.journal.ReplayMessages(ctx, f.persistenceID, f.seqNr+1, ^uint64(0), 0, func(repr PersistentRepr) {
			if event, ok := repr.Payload.(E); ok {
				f.currentState, f.stateData = f.applyEventFn(f.currentState, f.stateData, event)
				f.seqNr = repr.SequenceNr
			}
		})
		if err != nil {
			f.Log().Error("PersistentFSM: recovery failed", "persistenceID", f.persistenceID, "error", err)
			if s, ok := f.System().(interface{ Stop(actor.Ref) }); ok {
				s.Stop(f.Self())
			}
			return
		}
	}

	f.recovering = false
	f.Log().Info("PersistentFSM: recovery complete",
		"persistenceID", f.persistenceID,
		"state", fmt.Sprintf("%v", f.currentState),
		"seqNr", f.seqNr)

	// replay stashed commands
	stash := f.stash
	f.stash = nil
	for _, msg := range stash {
		f.dispatch(msg)
	}
}

func (f *PersistentFSM[S, D, E]) dispatch(msg any) {
	handler, ok := f.handlers[f.currentState]
	ev := FSMEvent[D]{Msg: msg, Data: f.stateData}

	var result FSMStateResult[S, D, E]

	if ok {
		result = handler(ev)
	}

	if !ok || result.Unhandled {
		if f.unhandledHandler != nil {
			result = f.unhandledHandler(ev)
		} else {
			f.Log().Warn("PersistentFSM: unhandled message",
				"state", fmt.Sprintf("%v", f.currentState),
				"msgType", fmt.Sprintf("%T", msg))
			return
		}
	}

	if result.Stop {
		f.Log().Info("PersistentFSM: stopping", "state", fmt.Sprintf("%v", f.currentState))
		if s, ok := f.System().(interface{ Stop(actor.Ref) }); ok {
			s.Stop(f.Self())
		}
		return
	}

	// Persist events before applying the transition
	if len(result.Events) > 0 {
		if err := f.persistEvents(result.Events); err != nil {
			f.Log().Error("PersistentFSM: persist failed", "error", err)
			return
		}
	}

	// Apply transition
	from := f.currentState
	f.currentState = result.NextState
	f.stateData = result.NextData

	if !reflect.DeepEqual(from, result.NextState) {
		f.Log().Info("PersistentFSM: state transition",
			"from", fmt.Sprintf("%v", from),
			"to", fmt.Sprintf("%v", result.NextState))
		for _, t := range f.transitions {
			t(from, result.NextState)
		}
	}

	// Snapshot AFTER the transition so the saved (state, data) pair reflects
	// the post-transition view. snapshot-after fires only when the configured
	// number of new events have been persisted since the last snapshot.
	if len(result.Events) > 0 {
		f.maybeSaveSnapshot(context.Background(), len(result.Events))
	}
}

func (f *PersistentFSM[S, D, E]) persistEvents(events []E) error {
	ctx := context.Background()
	reprs := make([]PersistentRepr, 0, len(events))
	for _, e := range events {
		f.seqNr++
		reprs = append(reprs, PersistentRepr{
			PersistenceID: f.persistenceID,
			SequenceNr:    f.seqNr,
			Payload:       e,
		})
	}
	if err := f.journal.AsyncWriteMessages(ctx, reprs); err != nil {
		return err
	}
	// Note: maybeSaveSnapshot is intentionally invoked by dispatch() after the
	// state transition has been applied, so the snapshot reflects post-event
	// (state, data). Calling it here would capture stale state.
	return nil
}

// maybeSaveSnapshot implements pekko.persistence.fsm.snapshot-after.  Each
// successful event-persist call increments the events-since-snapshot counter;
// once the counter crosses the configured threshold and a snapshot store is
// available, the current (state, data) pair is written and the counter resets.
func (f *PersistentFSM[S, D, E]) maybeSaveSnapshot(ctx context.Context, addedEvents int) {
	if f.snapshotStore == nil || addedEvents <= 0 {
		return
	}
	threshold := f.effectiveSnapshotAfter()
	if threshold <= 0 {
		return
	}
	f.eventsSinceSnapshot += addedEvents
	if f.eventsSinceSnapshot < threshold {
		return
	}
	meta := SnapshotMetadata{
		PersistenceID: f.persistenceID,
		SequenceNr:    f.seqNr,
		Timestamp:     time.Now().UnixNano(),
	}
	payload := FSMSnapshot[S, D]{State: f.currentState, Data: f.stateData}
	if err := f.snapshotStore.SaveSnapshot(ctx, meta, payload); err != nil {
		f.Log().Error("PersistentFSM: snapshot save failed",
			"persistenceID", f.persistenceID,
			"seqNr", f.seqNr,
			"error", err)
		return
	}
	f.eventsSinceSnapshot = 0
}

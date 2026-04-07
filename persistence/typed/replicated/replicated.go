/*
 * replicated.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package replicated

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	ptypes "github.com/sopranoworks/gekka/persistence/typed"
)

// ReplicaId identifies a data center / replica in a replicated event sourcing setup.
type ReplicaId string

// ReplicationId uniquely identifies a replicated entity across all replicas.
type ReplicationId struct {
	TypeHint string
	EntityId string
	ReplicaId
}

// PersistenceId returns the journal persistence ID for this replica.
// Format: "<TypeHint>|<EntityId>|<ReplicaId>"
func (r ReplicationId) PersistenceId() string {
	return fmt.Sprintf("%s|%s|%s", r.TypeHint, r.EntityId, r.ReplicaId)
}

// ReplicatedEvent wraps an event with its origin replica metadata.
type ReplicatedEvent[E any] struct {
	Event    E
	Origin   ReplicaId
	SeqNr    uint64
}

// ReplicaConfig maps a replica to its journal for replication reads.
type ReplicaConfig struct {
	ReplicaId ReplicaId
	Journal   persistence.Journal
}

// ReplicatedEventSourcing defines a behavior for a replicated persistent actor.
// Events from all replicas are merged; the user-provided EventHandler must be
// conflict-free (CRDT-style) since events from different replicas may arrive
// in different orders.
type ReplicatedEventSourcing[Command any, Event any, State any] struct {
	ReplicationId ReplicationId

	// Local journal for this replica's writes and recovery.
	Journal       persistence.Journal
	SnapshotStore persistence.SnapshotStore

	// AllReplicas lists every replica (including self) and its journal.
	AllReplicas []ReplicaConfig

	InitialState   State
	CommandHandler func(typed.TypedContext[Command], State, Command) ptypes.Effect[Event, State]
	EventHandler   func(State, ReplicatedEvent[Event]) State

	// ReplicationInterval controls how often we poll remote journals. Default: 1s.
	ReplicationInterval time.Duration
}

// replicatedActor is the internal actor implementation.
type replicatedActor[Command any, Event any, State any] struct {
	actor.BaseActor
	behavior *ReplicatedEventSourcing[Command, Event, State]
	state    State
	seqNr    uint64 // local sequence number

	// Track highest seen seqNr per replica for replication polling.
	mu              sync.Mutex
	replicaSeqNrs   map[ReplicaId]uint64
	recovering      bool
	tctx            typed.TypedContext[Command]
	stash           []Command
	cancel          context.CancelFunc
}

func NewReplicatedActor[Command any, Event any, State any](
	b *ReplicatedEventSourcing[Command, Event, State],
) actor.Actor {
	a := &replicatedActor[Command, Event, State]{
		BaseActor:     actor.NewBaseActor(),
		behavior:      b,
		state:         b.InitialState,
		recovering:    true,
		replicaSeqNrs: make(map[ReplicaId]uint64),
	}
	a.tctx = &replicatedTypedContext[Command, Event, State]{actor: a}
	return a
}

type replicatedTypedContext[C any, E any, S any] struct {
	actor *replicatedActor[C, E, S]
}

func (c *replicatedTypedContext[C, E, S]) Self() typed.TypedActorRef[C] {
	return typed.NewTypedActorRef[C](c.actor.Self())
}
func (c *replicatedTypedContext[C, E, S]) System() actor.ActorContext {
	return c.actor.System()
}
func (c *replicatedTypedContext[C, E, S]) Log() *slog.Logger {
	return c.actor.Log().Logger()
}
func (c *replicatedTypedContext[C, E, S]) Watch(target actor.Ref)   { /* no-op for now */ }
func (c *replicatedTypedContext[C, E, S]) Unwatch(target actor.Ref) { /* no-op */ }
func (c *replicatedTypedContext[C, E, S]) Stop(target actor.Ref)    { /* no-op */ }
func (c *replicatedTypedContext[C, E, S]) Passivate()               {}
func (c *replicatedTypedContext[C, E, S]) Timers() typed.TimerScheduler[C] { return nil }
func (c *replicatedTypedContext[C, E, S]) Stash() typed.StashBuffer[C]     { return nil }
func (c *replicatedTypedContext[C, E, S]) Sender() actor.Ref               { return nil }
func (c *replicatedTypedContext[C, E, S]) Ask(target actor.Ref, msgFactory func(actor.Ref) any, transform func(any, error) C) {
}
func (c *replicatedTypedContext[C, E, S]) Spawn(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().Spawn(behavior, name)
}
func (c *replicatedTypedContext[C, E, S]) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return c.actor.System().SpawnAnonymous(behavior)
}
func (c *replicatedTypedContext[C, E, S]) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return c.actor.System().SystemActorOf(behavior, name)
}

func (a *replicatedActor[Command, Event, State]) PreStart() {
	a.recover()
}

func (a *replicatedActor[Command, Event, State]) PostStop() {
	if a.cancel != nil {
		a.cancel()
	}
}

func (a *replicatedActor[Command, Event, State]) Receive(msg any) {
	if a.recovering {
		if cmd, ok := msg.(Command); ok {
			a.stash = append(a.stash, cmd)
		}
		return
	}

	// Handle replicated events from remote replicas.
	if re, ok := msg.(*replicatedEventEnvelope[Event]); ok {
		a.applyReplicatedEvent(re.event, re.origin, re.seqNr)
		return
	}

	if cmd, ok := msg.(Command); ok {
		effect := a.behavior.CommandHandler(a.tctx, a.state, cmd)
		if events, then, ok := ptypes.PersistEffectEvents[Event, State](effect); ok {
			a.persist(events, then)
		}
		return
	}
}

// replicatedEventEnvelope is an internal message for delivering replicated events.
type replicatedEventEnvelope[E any] struct {
	event  E
	origin ReplicaId
	seqNr  uint64
}

func (a *replicatedActor[Command, Event, State]) recover() {
	ctx := context.Background()
	selfReplicaId := a.behavior.ReplicationId.ReplicaId
	selfPersistenceId := a.behavior.ReplicationId.PersistenceId()

	// 1. Load snapshot (if available)
	if a.behavior.SnapshotStore != nil {
		snap, err := a.behavior.SnapshotStore.LoadSnapshot(ctx, selfPersistenceId, persistence.SnapshotSelectionCriteria{})
		if err == nil && snap != nil {
			if s, ok := snap.Snapshot.(State); ok {
				a.state = s
				a.seqNr = snap.Metadata.SequenceNr
			}
		}
	}

	// 2. Replay local events
	if a.behavior.Journal != nil {
		err := a.behavior.Journal.ReplayMessages(ctx, selfPersistenceId, a.seqNr+1, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
			if event, ok := repr.Payload.(Event); ok {
				re := ReplicatedEvent[Event]{
					Event:  event,
					Origin: selfReplicaId,
					SeqNr:  repr.SequenceNr,
				}
				a.state = a.behavior.EventHandler(a.state, re)
				a.seqNr = repr.SequenceNr
			}
		})
		if err != nil {
			a.Log().Error("Local recovery failed", "error", err)
			return
		}
	}

	// 3. Replay events from all remote replicas
	for _, rc := range a.behavior.AllReplicas {
		if rc.ReplicaId == selfReplicaId {
			continue
		}
		remotePid := fmt.Sprintf("%s|%s|%s",
			a.behavior.ReplicationId.TypeHint,
			a.behavior.ReplicationId.EntityId,
			rc.ReplicaId)
		var highestSeen uint64
		err := rc.Journal.ReplayMessages(ctx, remotePid, 1, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
			if event, ok := repr.Payload.(Event); ok {
				re := ReplicatedEvent[Event]{
					Event:  event,
					Origin: rc.ReplicaId,
					SeqNr:  repr.SequenceNr,
				}
				a.state = a.behavior.EventHandler(a.state, re)
				highestSeen = repr.SequenceNr
			}
		})
		if err != nil {
			a.Log().Error("Remote replica recovery failed", "replica", rc.ReplicaId, "error", err)
		}
		a.replicaSeqNrs[rc.ReplicaId] = highestSeen
	}

	a.recovering = false
	a.Log().Info("Replicated recovery completed",
		"localSeqNr", a.seqNr, "replicas", len(a.behavior.AllReplicas))

	// 4. Process stashed commands
	for _, cmd := range a.stash {
		a.Receive(cmd)
	}
	a.stash = nil

	// 5. Start replication polling loop
	a.startReplicationLoop()
}

func (a *replicatedActor[Command, Event, State]) persist(events []Event, then func(State)) {
	ctx := context.Background()
	selfPersistenceId := a.behavior.ReplicationId.PersistenceId()
	selfReplicaId := a.behavior.ReplicationId.ReplicaId

	var reprs []persistence.PersistentRepr
	for _, event := range events {
		a.seqNr++
		reprs = append(reprs, persistence.PersistentRepr{
			PersistenceID: selfPersistenceId,
			SequenceNr:    a.seqNr,
			Payload:       event,
			Tags:          []string{string(selfReplicaId)},
		})
	}

	if err := a.behavior.Journal.AsyncWriteMessages(ctx, reprs); err != nil {
		a.Log().Error("Failed to persist replicated events", "error", err)
		return
	}

	for _, event := range events {
		re := ReplicatedEvent[Event]{
			Event:  event,
			Origin: selfReplicaId,
			SeqNr:  a.seqNr,
		}
		a.state = a.behavior.EventHandler(a.state, re)
	}

	if then != nil {
		then(a.state)
	}
}

func (a *replicatedActor[Command, Event, State]) applyReplicatedEvent(event Event, origin ReplicaId, seqNr uint64) {
	a.mu.Lock()
	a.replicaSeqNrs[origin] = seqNr
	a.mu.Unlock()

	re := ReplicatedEvent[Event]{
		Event:  event,
		Origin: origin,
		SeqNr:  seqNr,
	}
	a.state = a.behavior.EventHandler(a.state, re)
}

func (a *replicatedActor[Command, Event, State]) startReplicationLoop() {
	interval := a.behavior.ReplicationInterval
	if interval == 0 {
		interval = time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel

	selfReplicaId := a.behavior.ReplicationId.ReplicaId

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.pollRemoteReplicas(ctx, selfReplicaId)
			}
		}
	}()
}

func (a *replicatedActor[Command, Event, State]) pollRemoteReplicas(ctx context.Context, selfReplicaId ReplicaId) {
	for _, rc := range a.behavior.AllReplicas {
		if rc.ReplicaId == selfReplicaId {
			continue
		}
		remotePid := fmt.Sprintf("%s|%s|%s",
			a.behavior.ReplicationId.TypeHint,
			a.behavior.ReplicationId.EntityId,
			rc.ReplicaId)

		a.mu.Lock()
		fromSeqNr := a.replicaSeqNrs[rc.ReplicaId] + 1
		a.mu.Unlock()

		err := rc.Journal.ReplayMessages(ctx, remotePid, fromSeqNr, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
			if event, ok := repr.Payload.(Event); ok {
				a.Self().Tell(&replicatedEventEnvelope[Event]{
					event:  event,
					origin: rc.ReplicaId,
					seqNr:  repr.SequenceNr,
				})
			}
		})
		if err != nil {
			a.Log().Warn("Replication poll failed", "replica", rc.ReplicaId, "error", err)
		}
	}
}

// State returns the current state of the replicated actor (for testing).
func (a *replicatedActor[Command, Event, State]) State() State {
	return a.state
}

// SeqNr returns the local sequence number (for testing).
func (a *replicatedActor[Command, Event, State]) SeqNr() uint64 {
	return a.seqNr
}

// SpawnReplicated creates a new replicated persistent actor.
func SpawnReplicated[Command any, Event any, State any](
	ctx actor.ActorContext,
	behavior *ReplicatedEventSourcing[Command, Event, State],
	name string,
	props ...actor.Props,
) (typed.TypedActorRef[Command], error) {
	p := actor.Props{
		New: func() actor.Actor { return NewReplicatedActor(behavior) },
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

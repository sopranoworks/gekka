/*
 * replicated_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package replicated

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	ptypes "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── test types ──

type addCmd struct{ Value int }
type addedEvt struct{ Value int }
type counterState struct{ Total int }

// ── mock infrastructure ──

type mockContext struct {
	actor.ActorContext
}

func (m *mockContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return &mockRef{path: "/user/" + name}, nil
}

func (m *mockContext) Stop(target actor.Ref) {}

func (m *mockContext) Spawn(behavior any, name string) (actor.Ref, error) {
	return &mockRef{path: "/user/" + name}, nil
}

func (m *mockContext) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return &mockRef{path: "/user/anon"}, nil
}

func (m *mockContext) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return &mockRef{path: "/system/" + name}, nil
}

type mockRef struct {
	actor.Ref
	path string
}

func (r *mockRef) Path() string                      { return r.path }
func (r *mockRef) Tell(msg any, sender ...actor.Ref) {}

// ── helpers ──

func newBehavior(replicaId ReplicaId, journal persistence.Journal, allReplicas []ReplicaConfig) *ReplicatedEventSourcing[addCmd, addedEvt, counterState] {
	return &ReplicatedEventSourcing[addCmd, addedEvt, counterState]{
		ReplicationId: ReplicationId{
			TypeHint:  "Counter",
			EntityId:  "c1",
			ReplicaId: replicaId,
		},
		Journal:     journal,
		AllReplicas: allReplicas,
		InitialState: counterState{Total: 0},
		CommandHandler: func(ctx typed.TypedContext[addCmd], state counterState, cmd addCmd) ptypes.Effect[addedEvt, counterState] {
			return ptypes.Persist[addedEvt, counterState](addedEvt(cmd))
		},
		EventHandler: func(state counterState, re ReplicatedEvent[addedEvt]) counterState {
			return counterState{Total: state.Total + re.Event.Value}
		},
		ReplicationInterval: 50 * time.Millisecond,
	}
}

func spawnActor(b *ReplicatedEventSourcing[addCmd, addedEvt, counterState]) *replicatedActor[addCmd, addedEvt, counterState] {
	a := NewReplicatedActor(b).(*replicatedActor[addCmd, addedEvt, counterState])
	a.SetSystem(&mockContext{})
	a.SetSelf(&mockRef{path: "/user/replicated-test"})
	return a
}

// ── Tests ──

func TestReplicationId_PersistenceId(t *testing.T) {
	rid := ReplicationId{TypeHint: "Counter", EntityId: "c1", ReplicaId: "DC-A"}
	assert.Equal(t, "Counter|c1|DC-A", rid.PersistenceId())
}

func TestReplicatedActor_LocalPersistAndRecover(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	replicas := []ReplicaConfig{{ReplicaId: "DC-A", Journal: journal}}
	b := newBehavior("DC-A", journal, replicas)

	a := spawnActor(b)
	a.PreStart()

	// Persist two commands locally
	a.Receive(addCmd{Value: 10})
	a.Receive(addCmd{Value: 20})

	assert.Equal(t, 30, a.state.Total)
	assert.Equal(t, uint64(2), a.seqNr)

	// Create a new actor to test recovery
	a2 := spawnActor(b)
	a2.PreStart()

	assert.Equal(t, 30, a2.state.Total)
	assert.Equal(t, uint64(2), a2.seqNr)
}

func TestReplicatedActor_CrossReplicaRecovery(t *testing.T) {
	journalA := persistence.NewInMemoryJournal()
	journalB := persistence.NewInMemoryJournal()

	ctx := context.Background()

	// Pre-populate journal A with events from replica DC-A
	require.NoError(t, journalA.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "Counter|c1|DC-A", SequenceNr: 1, Payload: addedEvt{Value: 10}, Tags: []string{"DC-A"}},
		{PersistenceID: "Counter|c1|DC-A", SequenceNr: 2, Payload: addedEvt{Value: 20}, Tags: []string{"DC-A"}},
	}))

	// Pre-populate journal B with events from replica DC-B
	require.NoError(t, journalB.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "Counter|c1|DC-B", SequenceNr: 1, Payload: addedEvt{Value: 5}, Tags: []string{"DC-B"}},
	}))

	replicas := []ReplicaConfig{
		{ReplicaId: "DC-A", Journal: journalA},
		{ReplicaId: "DC-B", Journal: journalB},
	}

	// Create actor for DC-A — should recover local (10+20) and remote (5)
	b := newBehavior("DC-A", journalA, replicas)
	a := spawnActor(b)
	a.PreStart()

	assert.Equal(t, 35, a.state.Total)
	assert.Equal(t, uint64(2), a.seqNr) // local seqNr from DC-A's journal
}

func TestReplicatedActor_ConcurrentEventsFromBothReplicas(t *testing.T) {
	journalA := persistence.NewInMemoryJournal()
	journalB := persistence.NewInMemoryJournal()

	replicas := []ReplicaConfig{
		{ReplicaId: "DC-A", Journal: journalA},
		{ReplicaId: "DC-B", Journal: journalB},
	}

	// Spawn actor A on DC-A
	bA := newBehavior("DC-A", journalA, replicas)
	aA := spawnActor(bA)
	aA.SetSelf(&mockRef{path: "/user/rep-a"})
	aA.PreStart()

	// Spawn actor B on DC-B
	bB := newBehavior("DC-B", journalB, replicas)
	aB := spawnActor(bB)
	aB.SetSelf(&mockRef{path: "/user/rep-b"})
	aB.PreStart()

	// Write on A
	aA.Receive(addCmd{Value: 100})
	assert.Equal(t, 100, aA.state.Total)

	// Write on B
	aB.Receive(addCmd{Value: 50})
	assert.Equal(t, 50, aB.state.Total)

	// Now recover a fresh actor on DC-A — should see both replicas' events
	aA2 := spawnActor(bA)
	aA2.SetSelf(&mockRef{path: "/user/rep-a2"})
	aA2.PreStart()
	assert.Equal(t, 150, aA2.state.Total) // 100 + 50
}

func TestReplicatedJournal_TagsEvents(t *testing.T) {
	inner := persistence.NewInMemoryJournal()
	rj := NewReplicatedJournal(inner, "DC-A")

	ctx := context.Background()
	err := rj.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "test", SequenceNr: 1, Payload: "hello"},
		{PersistenceID: "test", SequenceNr: 2, Payload: "world", Tags: []string{"existing-tag"}},
	})
	require.NoError(t, err)

	// Verify events are tagged with replica ID
	var events []persistence.PersistentRepr
	err = inner.ReplayMessages(ctx, "test", 1, ^uint64(0), 0, func(repr persistence.PersistentRepr) {
		events = append(events, repr)
	})
	require.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Contains(t, events[0].Tags, "DC-A")
	assert.Contains(t, events[1].Tags, "existing-tag")
	assert.Contains(t, events[1].Tags, "DC-A")
}

func TestReplicatedJournal_EventsByReplicaId(t *testing.T) {
	inner := persistence.NewInMemoryJournal()
	ctx := context.Background()

	// Write events from two replicas
	require.NoError(t, inner.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "test", SequenceNr: 1, Payload: "a-event", Tags: []string{"DC-A"}},
		{PersistenceID: "test", SequenceNr: 2, Payload: "b-event", Tags: []string{"DC-B"}},
		{PersistenceID: "test", SequenceNr: 3, Payload: "a-event2", Tags: []string{"DC-A"}},
	}))

	rj := NewReplicatedJournal(inner, "DC-A")

	var dcAEvents []persistence.PersistentRepr
	err := rj.EventsByReplicaId(ctx, "test", "DC-A", 1, func(repr persistence.PersistentRepr) {
		dcAEvents = append(dcAEvents, repr)
	})
	require.NoError(t, err)
	assert.Len(t, dcAEvents, 2)
	assert.Equal(t, "a-event", dcAEvents[0].Payload)
	assert.Equal(t, "a-event2", dcAEvents[1].Payload)
}

func TestReplicatedEventOriginTracking(t *testing.T) {
	journalA := persistence.NewInMemoryJournal()

	replicas := []ReplicaConfig{
		{ReplicaId: "DC-A", Journal: journalA},
	}

	// Track origins in event handler
	var origins []ReplicaId
	b := &ReplicatedEventSourcing[addCmd, addedEvt, counterState]{
		ReplicationId: ReplicationId{TypeHint: "Counter", EntityId: "c1", ReplicaId: "DC-A"},
		Journal:       journalA,
		AllReplicas:   replicas,
		InitialState:  counterState{},
		CommandHandler: func(ctx typed.TypedContext[addCmd], state counterState, cmd addCmd) ptypes.Effect[addedEvt, counterState] {
			return ptypes.Persist[addedEvt, counterState](addedEvt(cmd))
		},
		EventHandler: func(state counterState, re ReplicatedEvent[addedEvt]) counterState {
			origins = append(origins, re.Origin)
			return counterState{Total: state.Total + re.Event.Value}
		},
		ReplicationInterval: 50 * time.Millisecond,
	}

	a := spawnActor(b)
	a.PreStart()
	a.Receive(addCmd{Value: 7})

	require.Len(t, origins, 1)
	assert.Equal(t, ReplicaId("DC-A"), origins[0])
}

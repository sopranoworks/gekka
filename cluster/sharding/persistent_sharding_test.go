/*
 * persistent_sharding_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	cpersistence "github.com/sopranoworks/gekka/cluster/persistence"
)

// ── AccountActor ──────────────────────────────────────────────────────────────

// DepositCmd instructs AccountActor to credit the account by Amount.
type DepositCmd struct{ Amount int }

// GetBalanceCmd queries the current balance; the result is sent to Reply.
type GetBalanceCmd struct{ Reply chan int }

// DepositedEvent is the durable event persisted on each deposit.
type DepositedEvent struct{ Amount int }

// AccountActor is a minimal event-sourced actor modelling a bank account.
// PersistenceId() returns a placeholder; the entityIdAdapter in
// cluster_sharding.go overrides it with the sharding EntityId so that each
// account entity has its own journal partition.
type AccountActor struct {
	balance int
}

func (a *AccountActor) PersistenceId() string { return "account" }

func (a *AccountActor) OnCommand(ctx cpersistence.PersistContext, cmd any) {
	switch c := cmd.(type) {
	case DepositCmd:
		ctx.Persist(DepositedEvent(c), nil)
	case json.RawMessage:
		// Messages routed through ShardingEnvelope arrive as raw JSON bytes.
		// Decode into DepositCmd; ignore unknown shapes.
		var d DepositCmd
		if err := json.Unmarshal(c, &d); err == nil && d.Amount > 0 {
			ctx.Persist(DepositedEvent(d), nil)
		}
	case GetBalanceCmd:
		c.Reply <- a.balance
	}
}

func (a *AccountActor) OnEvent(event any) {
	if e, ok := event.(DepositedEvent); ok {
		a.balance += e.Amount
	}
}

func (a *AccountActor) OnSnapshot(_ any) {}

// ── accountDecoder ────────────────────────────────────────────────────────────

func accountDecoder(typeName string, raw json.RawMessage) (any, error) {
	if typeName == "DepositedEvent" {
		var e DepositedEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return e, nil
	}
	return nil, nil
}

// ── liveActorContext ──────────────────────────────────────────────────────────

// liveActorContext is a test ActorContext that calls PreStart on every spawned
// actor.  This enables PersistentActorWrapper to execute journal recovery
// synchronously inside unit tests, without a running actor system.
type liveActorContext struct {
	actor.ActorContext
	actors map[string]*liveRef
}

func newLiveActorContext() *liveActorContext {
	return &liveActorContext{actors: make(map[string]*liveRef)}
}

func (c *liveActorContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	a := props.New()
	ref := &liveRef{path: "/user/" + name, actor: a}
	if ba, ok := a.(interface {
		SetSystem(actor.ActorContext)
		SetSelf(actor.Ref)
	}); ok {
		ba.SetSystem(c)
		ba.SetSelf(ref)
	}
	if ps, ok := a.(interface{ PreStart() }); ok {
		ps.PreStart()
	}
	c.actors[name] = ref
	return ref, nil
}

func (c *liveActorContext) Stop(ref actor.Ref) {
	for k, v := range c.actors {
		if v.Path() == ref.Path() {
			delete(c.actors, k)
			return
		}
	}
}

func (c *liveActorContext) Watch(_, _ actor.Ref) {}

// liveRef holds an actor instance and dispatches Tell calls synchronously.
type liveRef struct {
	path  string
	actor actor.Actor
}

func (r *liveRef) Path() string { return r.path }
func (r *liveRef) Tell(msg any, sender ...actor.Ref) {
	var s actor.Ref
	if len(sender) > 0 {
		s = sender[0]
	}
	if ba, ok := r.actor.(interface{ SetSender(actor.Ref) }); ok {
		ba.SetSender(s)
	}
	r.actor.Receive(msg)
}

// ── TestPersistentSharding_CrossNodeRecovery ──────────────────────────────────

// TestPersistentSharding_CrossNodeRecovery is the primary Phase 7 integration
// test.  It verifies that a persistent sharded entity's state survives a
// handoff from one shard host to another.
//
// Scenario:
//  1. "Node A" starts a shard whose entity creator wraps AccountActor in a
//     PersistentActorWrapper backed by a shared FileJournal.
//  2. A DepositCmd{100} is routed to entity "account-1" via a ShardingEnvelope.
//     The entity spawns fresh (empty journal), processes the command, and
//     persists DepositedEvent{100} to the journal.
//  3. "Node B" starts a brand-new shard backed by the same FileJournal.
//  4. A trigger message is routed to "account-1" on Node B; this causes the
//     entity to be spawned for the first time on this node.
//     PersistentActorWrapper.PreStart() replays DepositedEvent{100} from the
//     journal before the trigger message is processed.
//  5. GetBalanceCmd is delivered directly to the recovered entity ref.
//  6. The reply must be 100.
func TestPersistentSharding_CrossNodeRecovery(t *testing.T) {
	dir := t.TempDir()
	journal := cpersistence.NewFileJournal(
		filepath.Join(dir, "accounts.jsonl"),
		accountDecoder,
	)

	factory := func(_ EntityId) cpersistence.PersistentActor {
		return &AccountActor{}
	}

	// makeEntityCreator builds the entity-creator closure used by each shard.
	// Each call captures its own liveActorContext so spawned actors are tracked
	// per-node.
	makeEntityCreator := func(lctx *liveActorContext) func(actor.ActorContext, EntityId) (actor.Ref, error) {
		return func(_ actor.ActorContext, id EntityId) (actor.Ref, error) {
			return lctx.ActorOf(actor.Props{
				New: func() actor.Actor {
					inner := &entityIdAdapter{
						PersistentActor: factory(id),
						entityId:        id,
					}
					return cpersistence.NewPersistentActorWrapper(inner, journal)
				},
			}, id)
		}
	}

	// ── Node A: deposit 100 to account-1 ─────────────────────────────────
	lctxA := newLiveActorContext()
	shardA := NewShard("Account", "shard-0", makeEntityCreator(lctxA), nil, ShardSettings{})
	actor.InjectSystem(shardA, lctxA)
	shardA.SetSelf(&mockRef{path: "/user/AccountRegion/shard-0"})
	shardA.PreStart()

	depositBytes, _ := json.Marshal(DepositCmd{Amount: 100})
	shardA.Receive(ShardingEnvelope{
		EntityId: "account-1",
		ShardId:  "shard-0",
		Message:  depositBytes,
	})

	if _, ok := shardA.entities["account-1"]; !ok {
		t.Fatal("Node A: account-1 was not spawned")
	}

	// ── Node B: handoff — entity recovers its state from the journal ──────
	lctxB := newLiveActorContext()
	shardB := NewShard("Account", "shard-0", makeEntityCreator(lctxB), nil, ShardSettings{})
	actor.InjectSystem(shardB, lctxB)
	shardB.SetSelf(&mockRef{path: "/user/AccountRegion/shard-0"})
	shardB.PreStart()

	// Route a trigger message to account-1 on Node B.  The entity has never
	// been spawned on this node, so the entity creator runs, which calls
	// PersistentActorWrapper.PreStart() (via liveActorContext.ActorOf) and
	// replays DepositedEvent{100} from the journal before the trigger message
	// is delivered.  The trigger shape `{}` is an unknown JSON object that
	// AccountActor silently ignores.
	shardB.Receive(ShardingEnvelope{
		EntityId: "account-1",
		ShardId:  "shard-0",
		Message:  json.RawMessage(`{}`),
	})

	entityRefB, ok := lctxB.actors["account-1"]
	if !ok {
		t.Fatal("Node B: account-1 was not spawned during recovery")
	}

	// Verify the recovered balance.
	reply := make(chan int, 1)
	entityRefB.Tell(GetBalanceCmd{Reply: reply})
	got := <-reply
	if got != 100 {
		t.Errorf("cross-node recovery: expected balance 100, got %d", got)
	}
}

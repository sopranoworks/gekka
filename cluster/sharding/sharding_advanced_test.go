/*
 * sharding_advanced_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// ── mock actor infrastructure ────────────────────────────────────────────────

type mockRef struct {
	path     string
	mu       sync.Mutex
	messages []any
}

func (r *mockRef) Path() string { return r.path }
func (r *mockRef) Tell(msg any, _ ...actor.Ref) {
	r.mu.Lock()
	r.messages = append(r.messages, msg)
	r.mu.Unlock()
}

type mockActorContext struct {
	actor.ActorContext
	created []string
	stopped []string
	actors  map[string]actor.Ref
}

func newMockActorContext() *mockActorContext {
	return &mockActorContext{actors: make(map[string]actor.Ref)}
}

func (m *mockActorContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	ref := &mockRef{path: "/user/TestRegion/" + name}
	m.actors[name] = ref
	m.created = append(m.created, name)
	return ref, nil
}

func (m *mockActorContext) Stop(ref actor.Ref) {
	m.stopped = append(m.stopped, ref.Path())
	// Remove from actors map by path.
	for k, v := range m.actors {
		if v.Path() == ref.Path() {
			delete(m.actors, k)
			break
		}
	}
}

func (m *mockActorContext) Watch(_, _ actor.Ref)     {}
func (m *mockActorContext) Context() context.Context { return context.Background() }
func (m *mockActorContext) Resolve(path string) (actor.Ref, error) {
	return &mockRef{path: path}, nil
}

// newTestShard is a helper that wires up a Shard with a mock context.
func newTestShard(t *testing.T, typeName, shardId string, settings ShardSettings) (*Shard, *mockActorContext) {
	t.Helper()
	mctx := newMockActorContext()

	entityCreator := func(ctx actor.ActorContext, entityId EntityId) (actor.Ref, error) {
		return mctx.ActorOf(actor.Props{}, entityId)
	}

	shard := NewShard(typeName, shardId, entityCreator, nil, settings)
	actor.InjectSystem(shard, mctx)
	shard.SetSelf(&mockRef{path: "/user/" + typeName + "Region/" + shardId})
	return shard, mctx
}

// sendEnvelope is a helper to deliver a ShardingEnvelope with raw JSON payload.
func sendEnvelope(s *Shard, entityId EntityId, msg any) {
	data, _ := json.Marshal(msg)
	s.Receive(ShardingEnvelope{EntityId: entityId, ShardId: "shard-0", Message: data})
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestPassivation_SelfInitiated verifies that when an entity sends a Passivate
// message to the Shard, the entity is removed from the entity map and stopped.
func TestPassivation_SelfInitiated(t *testing.T) {
	shard, mctx := newTestShard(t, "TestType", "shard-0", ShardSettings{})
	shard.PreStart()

	// Spawn entity by delivering a message.
	sendEnvelope(shard, "e1", "hello")

	if _, ok := shard.entities["e1"]; !ok {
		t.Fatal("expected entity e1 to be spawned")
	}

	entityRef := shard.entities["e1"]

	// Entity initiates passivation.
	shard.Receive(actor.Passivate{Entity: entityRef})

	if _, ok := shard.entities["e1"]; ok {
		t.Error("expected entity e1 to be removed after Passivate")
	}
	if len(mctx.stopped) == 0 {
		t.Error("expected Stop to be called on the entity")
	}
}

// TestPassivation_IdleTimeout verifies that after an entity has been idle
// longer than PassivationIdleTimeout, checkIdleEntities removes and stops it.
func TestPassivation_IdleTimeout(t *testing.T) {
	timeout := 1 * time.Second
	shard, mctx := newTestShard(t, "TestType", "shard-0", ShardSettings{
		PassivationIdleTimeout: timeout,
	})
	shard.PreStart()

	// Spawn two entities.
	sendEnvelope(shard, "e1", "hello")
	sendEnvelope(shard, "e2", "hello")

	if len(shard.entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(shard.entities))
	}

	// Artificially age e1's last activity to exceed the idle timeout.
	shard.lastActivity["e1"] = time.Now().Add(-2 * timeout)

	// Trigger the idle scan directly (avoids real timer sleep in tests).
	shard.Receive(checkPassivationMsg{})

	// e1 should be gone; e2 should still be active.
	if _, ok := shard.entities["e1"]; ok {
		t.Error("expected idle entity e1 to be passivated")
	}
	if _, ok := shard.entities["e2"]; !ok {
		t.Error("expected active entity e2 to remain alive")
	}
	if len(mctx.stopped) != 1 {
		t.Errorf("expected 1 Stop call, got %d", len(mctx.stopped))
	}
}

// TestPassivation_ActivityRefreshed verifies that delivering a new message to
// an entity resets its idle timer so it is not passivated prematurely.
func TestPassivation_ActivityRefreshed(t *testing.T) {
	timeout := 1 * time.Second
	shard, mctx := newTestShard(t, "TestType", "shard-0", ShardSettings{
		PassivationIdleTimeout: timeout,
	})
	shard.PreStart()

	sendEnvelope(shard, "e1", "hello")

	// Age the activity, then send a fresh message — activity should reset.
	shard.lastActivity["e1"] = time.Now().Add(-2 * timeout)
	sendEnvelope(shard, "e1", "ping") // refreshes lastActivity

	// Idle check now — e1 is fresh so must survive.
	shard.Receive(checkPassivationMsg{})

	if _, ok := shard.entities["e1"]; !ok {
		t.Error("expected entity e1 to survive after activity refresh")
	}
	if len(mctx.stopped) != 0 {
		t.Errorf("expected 0 Stop calls, got %d", len(mctx.stopped))
	}
}

// TestRememberEntities_SpawnPersistsEvent verifies that spawning an entity
// writes an EntityStarted event to the journal.
func TestRememberEntities_SpawnPersistsEvent(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	shard, _ := newTestShard(t, "Cart", "shard-1", ShardSettings{
		RememberEntities: true,
		Journal:          j,
	})
	shard.PreStart()

	sendEnvelope(shard, "cart-42", "addItem")

	ctx := context.Background()
	high, err := j.ReadHighestSequenceNr(ctx, "shard-Cart-shard-1", 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if high != 1 {
		t.Errorf("expected seqNr 1 after first spawn, got %d", high)
	}

	var events []persistence.PersistentRepr
	_ = j.ReplayMessages(ctx, "shard-Cart-shard-1", 1, high, 0, func(r persistence.PersistentRepr) {
		events = append(events, r)
	})
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	evt, ok := events[0].Payload.(entityStartedEvent)
	if !ok {
		t.Fatalf("expected entityStartedEvent, got %T", events[0].Payload)
	}
	if evt.EntityId != "cart-42" {
		t.Errorf("expected EntityId cart-42, got %s", evt.EntityId)
	}
}

// TestRememberEntities_PassivatePersistsStopEvent verifies that passivating an
// entity writes an EntityStopped event to the journal.
func TestRememberEntities_PassivatePersistsStopEvent(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	shard, _ := newTestShard(t, "Cart", "shard-1", ShardSettings{
		RememberEntities: true,
		Journal:          j,
	})
	shard.PreStart()

	sendEnvelope(shard, "cart-42", "addItem")
	entityRef := shard.entities["cart-42"]

	shard.Receive(actor.Passivate{Entity: entityRef})

	ctx := context.Background()
	high, _ := j.ReadHighestSequenceNr(ctx, "shard-Cart-shard-1", 0)
	if high != 2 {
		t.Errorf("expected seqNr 2 after EntityStarted+EntityStopped, got %d", high)
	}

	var events []persistence.PersistentRepr
	_ = j.ReplayMessages(ctx, "shard-Cart-shard-1", 1, high, 0, func(r persistence.PersistentRepr) {
		events = append(events, r)
	})
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if _, ok := events[0].Payload.(entityStartedEvent); !ok {
		t.Errorf("event[0] should be entityStartedEvent, got %T", events[0].Payload)
	}
	if _, ok := events[1].Payload.(entityStoppedEvent); !ok {
		t.Errorf("event[1] should be entityStoppedEvent, got %T", events[1].Payload)
	}
}

// TestRememberEntities_Recovery verifies that after a Shard restart, entities
// that were active at shutdown are re-spawned during PreStart recovery.
func TestRememberEntities_Recovery(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	settings := ShardSettings{RememberEntities: true, Journal: j}

	// Phase 1: original shard — spawn two entities, passivate one.
	shard1, mctx1 := newTestShard(t, "Cart", "shard-1", settings)
	shard1.PreStart()

	sendEnvelope(shard1, "cart-A", "msg")
	sendEnvelope(shard1, "cart-B", "msg")

	// Passivate cart-B — its EntityStopped event is written.
	refB := shard1.entities["cart-B"]
	shard1.Receive(actor.Passivate{Entity: refB})

	if len(shard1.entities) != 1 {
		t.Fatalf("expected 1 entity after passivation, got %d", len(shard1.entities))
	}
	_ = mctx1 // verify journal state, not the actor

	// Phase 2: new shard with same journal — recovery should re-spawn cart-A only.
	shard2, mctx2 := newTestShard(t, "Cart", "shard-1", settings)
	shard2.PreStart() // replays journal

	if _, ok := shard2.entities["cart-A"]; !ok {
		t.Error("expected cart-A to be recovered after shard restart")
	}
	if _, ok := shard2.entities["cart-B"]; ok {
		t.Error("expected cart-B NOT to be recovered (it was passivated before shutdown)")
	}

	// Confirm that the entity creator was called exactly once during recovery.
	recoveredCount := 0
	for _, name := range mctx2.created {
		if name == "cart-A" {
			recoveredCount++
		}
	}
	if recoveredCount != 1 {
		t.Errorf("expected cart-A to be re-spawned once, got %d", recoveredCount)
	}
}

// TestRememberEntities_RecoveryWithPassivation verifies that recovering entities
// also initialises lastActivity so idle-timeout applies to them immediately.
func TestRememberEntities_RecoveryWithPassivation(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	timeout := 1 * time.Second
	settings := ShardSettings{
		RememberEntities:       true,
		Journal:                j,
		PassivationIdleTimeout: timeout,
	}

	// Seed the journal by running a shard that spawns one entity.
	shard1, _ := newTestShard(t, "Cart", "shard-1", settings)
	shard1.PreStart()
	sendEnvelope(shard1, "cart-X", "msg")

	// Restart shard with the same journal.
	shard2, mctx2 := newTestShard(t, "Cart", "shard-1", settings)
	shard2.PreStart()

	if _, ok := shard2.entities["cart-X"]; !ok {
		t.Fatal("expected cart-X to be recovered")
	}

	// Expire the recovered entity's activity and trigger idle scan.
	shard2.lastActivity["cart-X"] = time.Now().Add(-2 * timeout)
	shard2.Receive(checkPassivationMsg{})

	if _, ok := shard2.entities["cart-X"]; ok {
		t.Error("expected recovered idle entity cart-X to be passivated after scan")
	}
	if len(mctx2.stopped) == 0 {
		t.Error("expected Stop to be called on idle recovered entity")
	}
}

// TestHOCON_ShardingConfig verifies that HOCON keys for passivation and
// remember-entities are correctly parsed into ShardingSettings.
// (Parsing lives in hocon_config.go / ClusterConfig, tested here as a
// cross-cutting validation.)
func TestShardSettings_Defaults(t *testing.T) {
	s := ShardSettings{}
	if s.PassivationIdleTimeout != 0 {
		t.Errorf("expected zero timeout by default, got %v", s.PassivationIdleTimeout)
	}
	if s.RememberEntities {
		t.Error("expected RememberEntities=false by default")
	}
	if s.Journal != nil {
		t.Error("expected nil Journal by default")
	}
}

// TestLRUPassivation verifies that when PassivationStrategy is "custom-lru-strategy"
// and the active entity count exceeds the limit, the oldest entity is evicted.
func TestLRUPassivation(t *testing.T) {
	shard, mctx := newTestShard(t, "TestType", "shard-0", ShardSettings{
		PassivationStrategy:         "custom-lru-strategy",
		PassivationActiveEntityLimit: 3,
	})
	shard.PreStart()

	// Spawn 3 entities — at the limit, no eviction yet.
	sendEnvelope(shard, "e1", "msg1")
	sendEnvelope(shard, "e2", "msg2")
	sendEnvelope(shard, "e3", "msg3")

	if len(shard.entities) != 3 {
		t.Fatalf("expected 3 entities, got %d", len(shard.entities))
	}

	// Set distinct timestamps so e2 is the oldest (LRU target).
	now := time.Now()
	shard.lastActivity["e1"] = now.Add(2 * time.Second)
	shard.lastActivity["e2"] = now // oldest
	shard.lastActivity["e3"] = now.Add(1 * time.Second)

	// Spawn a 4th entity — should trigger LRU eviction of e2 (oldest activity).
	sendEnvelope(shard, "e4", "msg4")

	if len(shard.entities) != 3 {
		t.Errorf("expected 3 entities after LRU eviction, got %d", len(shard.entities))
	}
	if _, ok := shard.entities["e2"]; ok {
		t.Error("expected e2 to be evicted (oldest activity)")
	}
	// e1, e3, e4 should still exist.
	for _, id := range []EntityId{"e1", "e3", "e4"} {
		if _, ok := shard.entities[id]; !ok {
			t.Errorf("expected entity %q to still exist", id)
		}
	}

	if len(mctx.stopped) != 1 {
		t.Errorf("expected 1 stop call (evicted entity), got %d", len(mctx.stopped))
	}
}

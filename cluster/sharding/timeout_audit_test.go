/*
 * timeout_audit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Phase 7.2 timeout audit — tests the runtime consumers of:
//   - pekko.cluster.sharding.waiting-for-state-timeout    (state recovery)
//   - pekko.cluster.sharding.updating-state-timeout       (state writes)
//   - pekko.cluster.sharding.shard-region-query-timeout   (region pending-msg drop)
//
// Each timeout has a real production code-path consumer so the field is
// not just plumbed-but-unread.

package sharding

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// slowJournal wraps an in-memory journal and inserts a configurable delay
// before each Read/Replay/Write call so the timeout consumers in shard.go
// can be exercised deterministically.
type slowJournal struct {
	inner persistence.Journal
	delay time.Duration

	writeCalls  int32
	readCalls   int32
	replayCalls int32
}

func (j *slowJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	atomic.AddInt32(&j.writeCalls, 1)
	select {
	case <-time.After(j.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return j.inner.AsyncWriteMessages(ctx, messages)
}

func (j *slowJournal) ReadHighestSequenceNr(ctx context.Context, persistenceID string, fromSeq uint64) (uint64, error) {
	atomic.AddInt32(&j.readCalls, 1)
	select {
	case <-time.After(j.delay):
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return j.inner.ReadHighestSequenceNr(ctx, persistenceID, fromSeq)
}

func (j *slowJournal) ReplayMessages(ctx context.Context, persistenceID string, fromSeq, toSeq uint64, max uint64, replay func(persistence.PersistentRepr)) error {
	atomic.AddInt32(&j.replayCalls, 1)
	select {
	case <-time.After(j.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return j.inner.ReplayMessages(ctx, persistenceID, fromSeq, toSeq, max, replay)
}

func (j *slowJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceID string, toSeq uint64) error {
	return j.inner.AsyncDeleteMessagesTo(ctx, persistenceID, toSeq)
}

// TestShard_WaitingForStateTimeout_BoundsJournalRecovery verifies that
// recoverFromJournal honors waiting-for-state-timeout: a slow journal
// whose response time exceeds the timeout produces a logged warning and
// recovery proceeds (with empty active set) rather than blocking.
func TestShard_WaitingForStateTimeout_BoundsJournalRecovery(t *testing.T) {
	// First, populate an in-memory journal with one EntityStarted event so
	// recovery has something to read once the slow path lifts the delay.
	innerJournal := persistence.NewInMemoryJournal()
	_ = innerJournal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{{
		PersistenceID: "shard-Cart-shard-0",
		SequenceNr:    1,
		Payload:       entityStartedEvent{EntityId: "cart-A"},
	}})

	// Slow wrapper: 200ms delay; timeout: 50ms — the read MUST time out.
	slow := &slowJournal{inner: innerJournal, delay: 200 * time.Millisecond}

	settings := ShardSettings{
		RememberEntities:       true,
		Journal:                slow,
		WaitingForStateTimeout: 50 * time.Millisecond,
	}
	shard, _ := newTestShard(t, "Cart", "shard-0", settings)

	start := time.Now()
	shard.PreStart()
	elapsed := time.Since(start)

	// PreStart must have returned in well under the journal delay.
	if elapsed > 150*time.Millisecond {
		t.Errorf("PreStart took %v; expected ≤150ms when waiting-for-state-timeout=50ms",
			elapsed)
	}
	// No entities should have been recovered (the read timed out).
	if got := len(shard.entities); got != 0 {
		t.Errorf("expected 0 recovered entities under timeout; got %d", got)
	}
}

// slowShardStore is a ShardStore whose operations sleep for `delay` before
// returning so the updating-state-timeout / waiting-for-state-timeout
// consumers can be exercised against a synchronous backend.
type slowShardStore struct {
	delay time.Duration

	getCalls int32
	addCalls int32
	rmCalls  int32
}

func (s *slowShardStore) AddEntity(_ ShardId, _ EntityId) error {
	atomic.AddInt32(&s.addCalls, 1)
	time.Sleep(s.delay)
	return nil
}

func (s *slowShardStore) RemoveEntity(_ ShardId, _ EntityId) error {
	atomic.AddInt32(&s.rmCalls, 1)
	time.Sleep(s.delay)
	return nil
}

func (s *slowShardStore) GetEntities(_ ShardId) ([]EntityId, error) {
	atomic.AddInt32(&s.getCalls, 1)
	time.Sleep(s.delay)
	return []EntityId{"slow-1"}, nil
}

// TestShard_WaitingForStateTimeout_BoundsStoreRecovery verifies that
// recoverFromStore honors waiting-for-state-timeout via a goroutine-based
// guard: a slow store whose response time exceeds the timeout produces a
// logged warning and recovery proceeds (with empty active set) rather
// than blocking PreStart.
func TestShard_WaitingForStateTimeout_BoundsStoreRecovery(t *testing.T) {
	store := &slowShardStore{delay: 200 * time.Millisecond}
	settings := ShardSettings{
		RememberEntities:       true,
		Store:                  store,
		WaitingForStateTimeout: 50 * time.Millisecond,
	}
	shard, _ := newTestShard(t, "Cart", "shard-0", settings)

	start := time.Now()
	shard.PreStart()
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond {
		t.Errorf("PreStart took %v; expected ≤150ms when waiting-for-state-timeout=50ms",
			elapsed)
	}
	if got := len(shard.entities); got != 0 {
		t.Errorf("expected 0 recovered entities (store load timed out); got %d", got)
	}
}

// TestShard_UpdatingStateTimeout_BoundsJournalWrite verifies that
// appendPersistEvent honors updating-state-timeout: a slow journal whose
// AsyncWriteMessages exceeds the timeout returns control to the Shard
// promptly with a logged warning.
func TestShard_UpdatingStateTimeout_BoundsJournalWrite(t *testing.T) {
	slow := &slowJournal{
		inner: persistence.NewInMemoryJournal(),
		delay: 200 * time.Millisecond,
	}
	settings := ShardSettings{
		RememberEntities:     true,
		Journal:              slow,
		UpdatingStateTimeout: 50 * time.Millisecond,
		// EventSourcedMaxUpdatesPerWrite=0 to force per-event writes (the
		// path that uses the timeout-bounded write helper).
	}
	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	start := time.Now()
	// Trigger an EntityStarted persist via the helper.
	shard.appendPersistEvent(entityStartedEvent{EntityId: "cart-A"})
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond {
		t.Errorf("appendPersistEvent took %v; expected ≤150ms when updating-state-timeout=50ms",
			elapsed)
	}
	if got := atomic.LoadInt32(&slow.writeCalls); got != 1 {
		t.Errorf("writeCalls = %d, want 1 (the one we initiated)", got)
	}
}

// TestShard_TimeoutDefaults verifies that unset timeouts fall back to
// Pekko-equivalent defaults (waiting-for-state=2s, updating-state=5s).
func TestShard_TimeoutDefaults(t *testing.T) {
	settings := ShardSettings{}
	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	if got, want := shard.resolveWaitingForStateTimeout(), 2*time.Second; got != want {
		t.Errorf("resolveWaitingForStateTimeout default = %v, want %v", got, want)
	}
	if got, want := shard.resolveUpdatingStateTimeout(), 5*time.Second; got != want {
		t.Errorf("resolveUpdatingStateTimeout default = %v, want %v", got, want)
	}
}

// TestShardRegion_QueryTimeoutDropsStalePending verifies that the region
// drops pending messages whose age exceeds shard-region-query-timeout when
// the retry tick fires, instead of buffering them forever while the
// coordinator is unreachable.
func TestShardRegion_QueryTimeoutDropsStalePending(t *testing.T) {
	mctx := newMockActorContext()
	regionRef := &mockRef{path: "/user/TestRegion"}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{
			RetryInterval:           20 * time.Millisecond,
			ShardRegionQueryTimeout: 60 * time.Millisecond,
		},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// Buffer a message — its home is unknown so it goes into pendingMessages.
	region.Receive(routeMsg{EntityId: "e1", Body: "first"})

	// First retry tick: still within timeout — message should remain.
	time.Sleep(30 * time.Millisecond)
	region.Receive(retryShardHomeTickMsg{})

	shardId := computeShardId("e1")
	if got := len(region.pendingMessages[shardId]); got != 1 {
		t.Fatalf("expected pending message to survive first retry tick; len=%d", got)
	}

	// Second retry tick after timeout — message must be dropped.
	time.Sleep(50 * time.Millisecond)
	region.Receive(retryShardHomeTickMsg{})

	if got := len(region.pendingMessages[shardId]); got != 0 {
		t.Errorf("shard-region-query-timeout: expected pending queue drained after timeout; len=%d", got)
	}
}

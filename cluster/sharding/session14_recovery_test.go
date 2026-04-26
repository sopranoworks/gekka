/*
 * session14_recovery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Tests for Round-2 session 14 — sharding retry/backoff (part 2).
//
// Covered HOCON paths and their behavior:
//   - pekko.cluster.sharding.entity-recovery-strategy            → "all" vs "constant"
//   - pekko.cluster.sharding.entity-recovery-constant-rate-strategy.frequency
//   - pekko.cluster.sharding.entity-recovery-constant-rate-strategy.number-of-entities
//   - waiting-for-state-timeout, updating-state-timeout, shard-region-query-timeout,
//     coordinator-state.write-majority-plus, coordinator-state.read-majority-plus
//     → plumbing-only round-trip (consumers wired in later sessions / DData paths).

package sharding

import (
	"sort"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// memShardStore is a test ShardStore that holds a fixed entity set for a
// single shard. It implements the ShardStore interface.
type memShardStore struct {
	shardID  ShardId
	entities []EntityId
}

func (s *memShardStore) AddEntity(_ ShardId, _ EntityId) error    { return nil }
func (s *memShardStore) RemoveEntity(_ ShardId, _ EntityId) error { return nil }
func (s *memShardStore) GetEntities(shardID ShardId) ([]EntityId, error) {
	if shardID != s.shardID {
		return nil, nil
	}
	out := make([]EntityId, len(s.entities))
	copy(out, s.entities)
	return out, nil
}

// TestEntityRecoveryStrategy_AllSpawnsEverythingAtOnce verifies the default
// "all" strategy spawns every remembered entity inside PreStart with no
// pending queue.
func TestEntityRecoveryStrategy_AllSpawnsEverythingAtOnce(t *testing.T) {
	store := &memShardStore{
		shardID:  "shard-0",
		entities: []EntityId{"e1", "e2", "e3", "e4", "e5", "e6", "e7"},
	}
	settings := ShardSettings{
		RememberEntities:       true,
		Store:                  store,
		EntityRecoveryStrategy: EntityRecoveryStrategyAll,
	}
	shard, mctx := newTestShard(t, "TestType", "shard-0", settings)
	shard.PreStart()

	if got, want := len(mctx.created), len(store.entities); got != want {
		t.Errorf("strategy=all: spawned %d entities, want %d (created=%v)", got, want, mctx.created)
	}
	if len(shard.pendingRecovery) != 0 {
		t.Errorf("strategy=all: pendingRecovery must be empty, got %v", shard.pendingRecovery)
	}
}

// TestEntityRecoveryStrategy_ConstantSpawnsFirstBatchAndDefersRest verifies
// the "constant" strategy spawns only number-of-entities entities synchronously
// and queues the rest for later batches driven by entityRecoveryTickMsg.
func TestEntityRecoveryStrategy_ConstantSpawnsFirstBatchAndDefersRest(t *testing.T) {
	ids := []EntityId{"e1", "e2", "e3", "e4", "e5", "e6", "e7"}
	store := &memShardStore{shardID: "shard-0", entities: ids}
	settings := ShardSettings{
		RememberEntities:                           true,
		Store:                                      store,
		EntityRecoveryStrategy:                     EntityRecoveryStrategyConstant,
		EntityRecoveryConstantRateNumberOfEntities: 3,
		// Use a long frequency to keep the timer dormant for the test;
		// drainEntityRecoveryBatch is invoked synchronously below.
		EntityRecoveryConstantRateFrequency: 1 * time.Hour,
	}
	shard, mctx := newTestShard(t, "TestType", "shard-0", settings)
	shard.PreStart()

	if got, want := len(mctx.created), 3; got != want {
		t.Fatalf("first batch: spawned %d, want %d (created=%v)", got, want, mctx.created)
	}
	if got, want := len(shard.pendingRecovery), 4; got != want {
		t.Fatalf("pendingRecovery len = %d, want %d", got, want)
	}

	// Tick → next 3.
	shard.Receive(entityRecoveryTickMsg{})
	if got, want := len(mctx.created), 6; got != want {
		t.Fatalf("after 1st tick: spawned %d, want %d (created=%v)", got, want, mctx.created)
	}
	if got, want := len(shard.pendingRecovery), 1; got != want {
		t.Fatalf("after 1st tick: pendingRecovery len = %d, want %d", got, want)
	}

	// Final tick → last entity.
	shard.Receive(entityRecoveryTickMsg{})
	if got, want := len(mctx.created), 7; got != want {
		t.Fatalf("after 2nd tick: spawned %d, want %d (created=%v)", got, want, mctx.created)
	}
	if len(shard.pendingRecovery) != 0 {
		t.Errorf("after final tick: pendingRecovery must be empty, got %v", shard.pendingRecovery)
	}

	// Verify every original entity ID was spawned exactly once.
	got := append([]string(nil), mctx.created...)
	sort.Strings(got)
	want := []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"}
	if len(got) != len(want) {
		t.Fatalf("created set size mismatch: got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("created[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestEntityRecoveryStrategy_ConstantWithUnsetBatchUsesPekkoDefault verifies
// the Pekko default of 5 entities-per-batch is used when number-of-entities
// is unset.
func TestEntityRecoveryStrategy_ConstantWithUnsetBatchUsesPekkoDefault(t *testing.T) {
	ids := make([]EntityId, 12)
	for i := range ids {
		ids[i] = EntityId([]byte{'e', byte('0' + i%10)})
	}
	// Make all unique:
	ids = []EntityId{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	store := &memShardStore{shardID: "shard-0", entities: ids}
	settings := ShardSettings{
		RememberEntities:       true,
		Store:                  store,
		EntityRecoveryStrategy: EntityRecoveryStrategyConstant,
		// number-of-entities unset → default 5
		EntityRecoveryConstantRateFrequency: 1 * time.Hour,
	}
	shard, mctx := newTestShard(t, "TestType", "shard-0", settings)
	shard.PreStart()

	if got, want := len(mctx.created), 5; got != want {
		t.Errorf("default batch size: spawned %d, want %d", got, want)
	}
	if got, want := len(shard.pendingRecovery), 7; got != want {
		t.Errorf("default batch size: pendingRecovery = %d, want %d", got, want)
	}
}

// shardSelfRef is an actor.Ref that routes Tell directly back into a Shard's
// Receive method. Used so that time.AfterFunc-driven entityRecoveryTickMsg
// messages actually fire the recovery handler in tests (the default mockRef
// only captures messages without invoking Receive).
type shardSelfRef struct {
	path  string
	shard *Shard
}

func (r *shardSelfRef) Path() string { return r.path }
func (r *shardSelfRef) Tell(msg any, _ ...actor.Ref) {
	r.shard.Receive(msg)
}

// TestEntityRecoveryStrategy_ConstantPaceMeasured verifies frequency × batch
// size paces recovery as configured. Uses a short frequency so the test
// completes quickly. A self-ref routes time.AfterFunc-delivered ticks back
// into the shard so PreStart's scheduled timer drives real recovery.
func TestEntityRecoveryStrategy_ConstantPaceMeasured(t *testing.T) {
	ids := []EntityId{"e1", "e2", "e3", "e4", "e5", "e6"}
	store := &memShardStore{shardID: "shard-0", entities: ids}
	settings := ShardSettings{
		RememberEntities:                           true,
		Store:                                      store,
		EntityRecoveryStrategy:                     EntityRecoveryStrategyConstant,
		EntityRecoveryConstantRateNumberOfEntities: 2,
		EntityRecoveryConstantRateFrequency:        50 * time.Millisecond,
	}
	shard, mctx := newTestShard(t, "TestType", "shard-0", settings)
	// Replace the capture-only Self with one that routes ticks back into Receive.
	shard.SetSelf(&shardSelfRef{path: "/user/TestRegion/shard-0", shard: shard})

	start := time.Now()
	shard.PreStart()

	// First batch must be present immediately.
	if got, want := len(mctx.created), 2; got != want {
		t.Fatalf("first batch (sync): spawned %d, want %d", got, want)
	}

	// Wait for full drain — three batches × 50ms; first sync, then 2 ticks → ≥100ms.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(mctx.created) >= 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for all 6 entities to spawn (got %d)", len(mctx.created))
		}
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)
	if elapsed < 95*time.Millisecond {
		t.Errorf("constant-rate paced too fast: elapsed=%v, want ≥100ms (frequency × (batches-1))", elapsed)
	}
	if got := len(mctx.created); got != 6 {
		t.Errorf("final spawned count = %d, want 6", got)
	}
}

// TestShardSettings_Session14Plumbing verifies all 8 new ShardSettings fields
// round-trip into the Shard so downstream consumers see the configured values.
func TestShardSettings_Session14Plumbing(t *testing.T) {
	settings := ShardSettings{
		WaitingForStateTimeout:                     2 * time.Second,
		UpdatingStateTimeout:                       5 * time.Second,
		ShardRegionQueryTimeout:                    3 * time.Second,
		EntityRecoveryStrategy:                     EntityRecoveryStrategyConstant,
		EntityRecoveryConstantRateFrequency:        77 * time.Millisecond,
		EntityRecoveryConstantRateNumberOfEntities: 9,
		CoordinatorWriteMajorityPlus:               4,
		CoordinatorReadMajorityPlus:                6,
	}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	got := shard.settings
	if got.WaitingForStateTimeout != 2*time.Second {
		t.Errorf("WaitingForStateTimeout = %v", got.WaitingForStateTimeout)
	}
	if got.UpdatingStateTimeout != 5*time.Second {
		t.Errorf("UpdatingStateTimeout = %v", got.UpdatingStateTimeout)
	}
	if got.ShardRegionQueryTimeout != 3*time.Second {
		t.Errorf("ShardRegionQueryTimeout = %v", got.ShardRegionQueryTimeout)
	}
	if got.EntityRecoveryStrategy != EntityRecoveryStrategyConstant {
		t.Errorf("EntityRecoveryStrategy = %q", got.EntityRecoveryStrategy)
	}
	if got.EntityRecoveryConstantRateFrequency != 77*time.Millisecond {
		t.Errorf("EntityRecoveryConstantRateFrequency = %v", got.EntityRecoveryConstantRateFrequency)
	}
	if got.EntityRecoveryConstantRateNumberOfEntities != 9 {
		t.Errorf("EntityRecoveryConstantRateNumberOfEntities = %d", got.EntityRecoveryConstantRateNumberOfEntities)
	}
	if got.CoordinatorWriteMajorityPlus != 4 {
		t.Errorf("CoordinatorWriteMajorityPlus = %d", got.CoordinatorWriteMajorityPlus)
	}
	if got.CoordinatorReadMajorityPlus != 6 {
		t.Errorf("CoordinatorReadMajorityPlus = %d", got.CoordinatorReadMajorityPlus)
	}
}

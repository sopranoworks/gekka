/*
 * session13_retry_backoff_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Tests for Round-2 session 13 — sharding retry/backoff (part 1).
//
// Covered HOCON paths and their behavior:
//   - pekko.cluster.sharding.retry-interval         → re-tells GetShardHome
//   - pekko.cluster.sharding.buffer-size            → caps pendingMessages
//   - pekko.cluster.sharding.shard-failure-backoff  → delays cache clear
//   - pekko.cluster.sharding.coordinator-failure-backoff → delays re-register
//   - pekko.cluster.sharding.shard-start-timeout    → field plumbed
//   - pekko.cluster.sharding.entity-restart-backoff → field plumbed

package sharding

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TestShardRegion_BufferSizeCap verifies that once BufferSize messages are
// queued for a single shard whose home is unknown, additional messages are
// dropped (not appended to pendingMessages).
func TestShardRegion_BufferSizeCap(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{BufferSize: 3},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// Send 5 messages for the same entity (same shard); home is unknown so
	// all messages buffer. With BufferSize=3, only the first 3 are kept.
	for i := 0; i < 5; i++ {
		region.Receive(routeMsg{EntityId: "e1", Body: "msg"})
	}

	shardId := computeShardId("e1")
	if got, want := len(region.pendingMessages[shardId]), 3; got != want {
		t.Errorf("pendingMessages[%q] len = %d, want %d (BufferSize cap)", shardId, got, want)
	}
}

// TestShardRegion_BufferSizeUnboundedWhenZero verifies that BufferSize=0
// preserves legacy unbounded behavior (all messages buffer).
func TestShardRegion_BufferSizeUnboundedWhenZero(t *testing.T) {
	mctx := newMockActorContext()
	regionRef := &mockRef{path: "/user/TestRegion"}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{BufferSize: 0}, // unbounded
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	for i := 0; i < 50; i++ {
		region.Receive(routeMsg{EntityId: "e1", Body: "msg"})
	}

	shardId := computeShardId("e1")
	if got := len(region.pendingMessages[shardId]); got != 50 {
		t.Errorf("pendingMessages len = %d, want 50 (unbounded)", got)
	}
}

// TestShardRegion_RetryIntervalRetriesPendingHomes verifies that when shards
// are still pending, retryPendingHomes re-asks the coordinator.
func TestShardRegion_RetryIntervalRetriesPendingHomes(t *testing.T) {
	mctx := newMockActorContext()
	regionRef := &mockRef{path: "/user/TestRegion"}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{RetryInterval: 50 * time.Millisecond},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// Send a message for an entity — home is unknown, so it buffers and the
	// region asks the coordinator once.
	region.Receive(routeMsg{EntityId: "e1", Body: "first"})
	if len(coordRef.messages) != 1 {
		t.Fatalf("expected 1 GetShardHome after first message, got %d", len(coordRef.messages))
	}

	// Trigger the retry tick directly (avoids real timer sleep). The region
	// should re-ask for any pending shard.
	region.Receive(retryShardHomeTickMsg{})
	if len(coordRef.messages) != 2 {
		t.Errorf("expected 2 GetShardHome after retry tick, got %d", len(coordRef.messages))
	}
}

// TestShardRegion_RetryIntervalSkipsKnownHomes verifies that retryPendingHomes
// does not re-ask for shards whose home is already known.
func TestShardRegion_RetryIntervalSkipsKnownHomes(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{RetryInterval: 50 * time.Millisecond},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	region.Receive(routeMsg{EntityId: "e1", Body: "first"})
	req := coordRef.messages[0].(GetShardHome)
	region.Receive(ShardHome{ShardId: req.ShardId, RegionPath: regionPath})

	// Home is known and pending list cleared. A retry tick should not re-ask.
	prev := len(coordRef.messages)
	region.Receive(retryShardHomeTickMsg{})
	if len(coordRef.messages) != prev {
		t.Errorf("retry tick must not re-ask when home is known; coordinator messages = %d, want %d", len(coordRef.messages), prev)
	}
}

// terminatedStub is a minimal actor.TerminatedMessage implementation used by
// shard-failure-backoff tests to simulate a shard actor stopping.
type terminatedStub struct{ ref actor.Ref }

func (t terminatedStub) TerminatedActor() actor.Ref { return t.ref }

// TestShardRegion_ShardFailureBackoff_DelaysHomeCacheClear verifies that the
// region waits ShardFailureBackoff after a Shard terminates before clearing
// the cached home path. The clear is triggered by shardFailureBackoffElapsedMsg.
func TestShardRegion_ShardFailureBackoff_DelaysHomeCacheClear(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{ShardFailureBackoff: 1 * time.Hour},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	region.Receive(routeMsg{EntityId: "e1", Body: "first"})
	req := coordRef.messages[0].(GetShardHome)
	region.Receive(ShardHome{ShardId: req.ShardId, RegionPath: regionPath})
	shardRef := mctx.actors[mctx.created[0]].(*mockRef)

	if _, ok := region.shardHomePaths[req.ShardId]; !ok {
		t.Fatal("expected shard home cached")
	}

	// Simulate the shard terminating. With backoff=1h, the home cache must
	// remain populated; only an explicit elapsed-msg clears it.
	region.Receive(terminatedStub{ref: shardRef})

	if _, ok := region.shardHomePaths[req.ShardId]; !ok {
		t.Errorf("expected home cache to remain (backoff not yet elapsed)")
	}

	// Deliver the elapsed signal (simulating time.AfterFunc firing).
	region.Receive(shardFailureBackoffElapsedMsg{ShardId: req.ShardId})
	if _, ok := region.shardHomePaths[req.ShardId]; ok {
		t.Errorf("expected home cache cleared after backoff elapsed")
	}
}

// TestShardRegion_CoordinatorFailureBackoff_TriggersReRegister verifies that
// after CoordinatorFailureBackoff elapses, the region re-sends RegisterRegion.
func TestShardRegion_CoordinatorFailureBackoff_TriggersReRegister(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef,
		ShardSettings{CoordinatorFailureBackoff: 50 * time.Millisecond},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	region.PreStart() // sends initial RegisterRegion
	if got := len(coordRef.messages); got != 1 {
		t.Fatalf("expected 1 message after PreStart, got %d", got)
	}
	if _, ok := coordRef.messages[0].(RegisterRegion); !ok {
		t.Fatalf("first message should be RegisterRegion, got %T", coordRef.messages[0])
	}

	// Simulate coordinator-failure-backoff elapsed signal directly.
	region.Receive(coordinatorFailureBackoffElapsedMsg{})

	if got := len(coordRef.messages); got != 2 {
		t.Errorf("expected 2 messages after backoff-elapsed (re-register), got %d", got)
	}
	if _, ok := coordRef.messages[1].(RegisterRegion); !ok {
		t.Errorf("second message should be RegisterRegion, got %T", coordRef.messages[1])
	}
}

// TestShardSettings_Plumbing verifies the new fields round-trip from
// ShardSettings into the ShardRegion struct so downstream consumers see the
// configured values. shard-start-timeout and entity-restart-backoff are
// plumbing-only in this session; full consumer wiring is part 2.
func TestShardSettings_Plumbing(t *testing.T) {
	settings := ShardSettings{
		RetryInterval:             2 * time.Second,
		BufferSize:                12345,
		ShardStartTimeout:         15 * time.Second,
		ShardFailureBackoff:       10 * time.Second,
		EntityRestartBackoff:      8 * time.Second,
		CoordinatorFailureBackoff: 5 * time.Second,
	}
	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) { return nil, nil },
		nil, hashExtractor, nil, settings,
	)

	got := region.shardSettings
	if got.RetryInterval != 2*time.Second {
		t.Errorf("RetryInterval = %v", got.RetryInterval)
	}
	if got.BufferSize != 12345 {
		t.Errorf("BufferSize = %d", got.BufferSize)
	}
	if got.ShardStartTimeout != 15*time.Second {
		t.Errorf("ShardStartTimeout = %v", got.ShardStartTimeout)
	}
	if got.ShardFailureBackoff != 10*time.Second {
		t.Errorf("ShardFailureBackoff = %v", got.ShardFailureBackoff)
	}
	if got.EntityRestartBackoff != 8*time.Second {
		t.Errorf("EntityRestartBackoff = %v", got.EntityRestartBackoff)
	}
	if got.CoordinatorFailureBackoff != 5*time.Second {
		t.Errorf("CoordinatorFailureBackoff = %v", got.CoordinatorFailureBackoff)
	}
}

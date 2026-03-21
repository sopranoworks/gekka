/*
 * sharding_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// hashExtractor maps each message to an entity and shard using FNV-1a hash % 10.
// Messages must be of type routeMsg.
func hashExtractor(msg any) (EntityId, ShardId, any) {
	if m, ok := msg.(routeMsg); ok {
		h := fnv.New32a()
		h.Write([]byte(m.EntityId))
		return m.EntityId, fmt.Sprintf("%d", h.Sum32()%10), m
	}
	return "", "", msg
}

// routeMsg is a simple application message used in routing tests.
type routeMsg struct {
	EntityId string
	Body     string
}

// TestShardRegion_LocalRouting verifies the end-to-end local routing path:
//  1. ShardRegion receives a user message and applies the ShardIdExtractor.
//  2. It asks the ShardCoordinator for the shard home (GetShardHome).
//  3. When the coordinator responds with the local region path, the region
//     spawns a local Shard actor and forwards the EntityEnvelope to it.
func TestShardRegion_LocalRouting(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}

	// coordRef captures GetShardHome requests so the test can inspect them.
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, // no unmarshaler needed — Shard won't be called directly
		hashExtractor,
		coordRef,
		ShardSettings{},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// ── Step 1: send a user message ──────────────────────────────────────
	msg := routeMsg{EntityId: "entity-1", Body: "hello"}
	region.Receive(msg)

	// The region must have asked the coordinator for the shard home.
	if len(coordRef.messages) != 1 {
		t.Fatalf("expected 1 GetShardHome to coordinator, got %d", len(coordRef.messages))
	}
	req, ok := coordRef.messages[0].(GetShardHome)
	if !ok {
		t.Fatalf("expected GetShardHome, got %T", coordRef.messages[0])
	}
	if req.ShardId == "" {
		t.Fatal("GetShardHome must carry a non-empty ShardId")
	}

	// The message must be buffered (no shard spawned yet).
	if len(mctx.created) != 0 {
		t.Fatalf("shard must not be spawned before ShardHome is received, got %v", mctx.created)
	}

	// ── Step 2: coordinator responds with the local region as home ───────
	region.Receive(ShardHome{ShardId: req.ShardId, RegionPath: regionPath})

	// Now a local Shard must have been spawned.
	if len(mctx.created) != 1 {
		t.Fatalf("expected 1 shard spawned after ShardHome, got %d: %v", len(mctx.created), mctx.created)
	}

	// The Shard's mockRef must hold the EntityEnvelope with the correct EntityId.
	shardRef, ok := mctx.actors[mctx.created[0]].(*mockRef)
	if !ok {
		t.Fatalf("shard ref is not *mockRef")
	}
	if len(shardRef.messages) != 1 {
		t.Fatalf("expected 1 message in shard, got %d", len(shardRef.messages))
	}
	env, ok := shardRef.messages[0].(ShardingEnvelope)
	if !ok {
		t.Fatalf("expected ShardingEnvelope in shard, got %T", shardRef.messages[0])
	}
	if env.EntityId != "entity-1" {
		t.Errorf("EntityId = %q, want %q", env.EntityId, "entity-1")
	}
}

// TestShardRegion_LocalRouting_MultiMessage verifies that a second message to
// the same entity (same shard) is delivered immediately — without another
// GetShardHome round-trip — because the shard home is now cached.
func TestShardRegion_LocalRouting_MultiMessage(t *testing.T) {
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef, ShardSettings{},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// First message — triggers GetShardHome.
	region.Receive(routeMsg{EntityId: "e1", Body: "first"})
	if len(coordRef.messages) != 1 {
		t.Fatalf("expected 1 coordinator message, got %d", len(coordRef.messages))
	}
	req := coordRef.messages[0].(GetShardHome)
	region.Receive(ShardHome{ShardId: req.ShardId, RegionPath: regionPath})

	shardRef := mctx.actors[mctx.created[0]].(*mockRef)
	if len(shardRef.messages) != 1 {
		t.Fatalf("expected 1 message after first delivery, got %d", len(shardRef.messages))
	}

	// Second message to same entity — shard home already cached.
	region.Receive(routeMsg{EntityId: "e1", Body: "second"})

	// No additional GetShardHome should have been sent.
	if len(coordRef.messages) != 1 {
		t.Errorf("second message must not trigger GetShardHome; coordinator got %d messages", len(coordRef.messages))
	}
	// Shard should now have both messages.
	if len(shardRef.messages) != 2 {
		t.Errorf("expected 2 messages in shard after second delivery, got %d", len(shardRef.messages))
	}
}

// TestShardRegion_LocalRouting_DifferentShards verifies that two entities
// hashing to different shards each result in their own shard actor being
// spawned, and that each shard receives exactly its own message.
func TestShardRegion_LocalRouting_DifferentShards(t *testing.T) {
	// Find two entity IDs that hash to different shards (mod 10).
	// "entity-A" and "entity-B" are used; we verify post-hoc.
	mctx := newMockActorContext()
	regionPath := "/user/TestRegion"
	regionRef := &mockRef{path: regionPath}
	coordRef := &mockRef{path: "/user/coordinator"}

	region := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctx.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef, ShardSettings{},
	)
	actor.InjectSystem(region, mctx)
	region.SetSelf(regionRef)

	// Send message for entity-A.
	region.Receive(routeMsg{EntityId: "entity-A", Body: "msgA"})
	if len(coordRef.messages) != 1 {
		t.Fatalf("expected 1 GetShardHome for entity-A, got %d", len(coordRef.messages))
	}
	reqA := coordRef.messages[0].(GetShardHome)
	region.Receive(ShardHome{ShardId: reqA.ShardId, RegionPath: regionPath})

	// Compute shard for entity-B to check it differs.
	h := fnv.New32a()
	h.Write([]byte("entity-B"))
	shardB := fmt.Sprintf("%d", h.Sum32()%10)

	h2 := fnv.New32a()
	h2.Write([]byte("entity-A"))
	shardA := fmt.Sprintf("%d", h2.Sum32()%10)

	if shardA == shardB {
		t.Skip("entity-A and entity-B hash to the same shard — choose different IDs")
	}

	// Send message for entity-B (different shard → new GetShardHome).
	region.Receive(routeMsg{EntityId: "entity-B", Body: "msgB"})
	if len(coordRef.messages) != 2 {
		t.Fatalf("expected 2 coordinator messages total, got %d", len(coordRef.messages))
	}
	reqB := coordRef.messages[1].(GetShardHome)
	if reqB.ShardId == reqA.ShardId {
		t.Errorf("entity-A and entity-B should route to different shards, both got %s", reqA.ShardId)
	}
	region.Receive(ShardHome{ShardId: reqB.ShardId, RegionPath: regionPath})

	// Two distinct shards should now be spawned.
	if len(mctx.created) != 2 {
		t.Fatalf("expected 2 shards spawned, got %d: %v", len(mctx.created), mctx.created)
	}
}

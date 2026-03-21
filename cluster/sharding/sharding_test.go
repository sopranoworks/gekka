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

// ── Phase 2 test infrastructure ───────────────────────────────────────────────

// routingMockContext extends mockActorContext with cross-node routing.
// When Resolve is called for a registered path, it returns a ref that
// delivers the message directly to the registered ShardRegion, simulating
// network transport between two nodes in the same process.
type routingMockContext struct {
	*mockActorContext
	routes map[string]actor.Ref // path → forwarding ref for a remote region
}

func newRoutingMockContext(routes map[string]actor.Ref) *routingMockContext {
	return &routingMockContext{
		mockActorContext: newMockActorContext(),
		routes:          routes,
	}
}

// Resolve returns a registered forwarding ref when available, otherwise falls
// back to a plain capture-only mockRef (matching the base mock's behaviour).
func (m *routingMockContext) Resolve(path string) (actor.Ref, error) {
	if ref, ok := m.routes[path]; ok {
		return ref, nil
	}
	return &mockRef{path: path}, nil
}

// regionForwardRef is an actor.Ref that directly invokes a ShardRegion's
// Receive method so tests can simulate cross-node message delivery without
// real TCP transport.
type regionForwardRef struct {
	path   string
	region *ShardRegion
}

func (r *regionForwardRef) Path() string { return r.path }
func (r *regionForwardRef) Tell(msg any, sender ...actor.Ref) {
	if len(sender) > 0 && sender[0] != nil {
		actor.InjectSender(r.region, sender[0])
	}
	r.region.Receive(msg)
	actor.InjectSender(r.region, nil)
}

// computeShardId returns the shard ID that hashExtractor assigns to entityId.
func computeShardId(entityId string) ShardId {
	h := fnv.New32a()
	h.Write([]byte(entityId))
	return fmt.Sprintf("%d", h.Sum32()%10)
}

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

// ── Phase 2: distributed routing tests ───────────────────────────────────────

// TestShardRegion_RemoteForwarding verifies the full cross-node routing path:
//
//  1. ShardRegion A receives a user message and asks the coordinator for
//     GetShardHome.
//  2. The coordinator responds: the shard is owned by ShardRegion B.
//  3. Region A resolves B's actor path and forwards the EntityEnvelope.
//  4. Region B (pre-seeded to know it is home for that shard) delivers the
//     envelope to its local Shard actor.
//
// Node isolation is simulated with separate mockActorContext instances.
// Cross-node message delivery uses regionForwardRef (see above).
func TestShardRegion_RemoteForwarding(t *testing.T) {
	const regionAPath = "/user/RegionA"
	const regionBPath = "/user/RegionB"

	// ── Node B ────────────────────────────────────────────────────────────
	mctxB := newMockActorContext()
	regionB := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctxB.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, nil /* no coordinator needed on B for this test */, ShardSettings{},
	)
	actor.InjectSystem(regionB, mctxB)
	regionB.SetSelf(&mockRef{path: regionBPath})

	// Pre-seed B with its own path as home for the target shard so it
	// delivers locally without another coordinator round-trip.
	targetShard := computeShardId("entity-1")
	regionB.Receive(ShardHome{ShardId: targetShard, RegionPath: regionBPath})

	// ── Node A ────────────────────────────────────────────────────────────
	// A's context routes Resolve(regionBPath) → regionForwardRef → regionB.Receive
	forwardB := &regionForwardRef{path: regionBPath, region: regionB}
	mctxA := newRoutingMockContext(map[string]actor.Ref{regionBPath: forwardB})
	coordRef := &mockRef{path: "/user/coordinator"}

	regionA := NewShardRegion("TestType",
		func(ctx actor.ActorContext, id EntityId) (actor.Ref, error) {
			return mctxA.ActorOf(actor.Props{}, id)
		},
		nil, hashExtractor, coordRef, ShardSettings{},
	)
	actor.InjectSystem(regionA, mctxA)
	regionA.SetSelf(&mockRef{path: regionAPath})

	// ── Step 1: send user message to node A ───────────────────────────────
	regionA.Receive(routeMsg{EntityId: "entity-1", Body: "hello-remote"})

	// Coordinator should be queried for shard home.
	if len(coordRef.messages) != 1 {
		t.Fatalf("expected 1 GetShardHome to coordinator, got %d", len(coordRef.messages))
	}
	req, ok := coordRef.messages[0].(GetShardHome)
	if !ok {
		t.Fatalf("expected GetShardHome, got %T", coordRef.messages[0])
	}
	if req.ShardId != targetShard {
		t.Errorf("GetShardHome.ShardId = %q, want %q", req.ShardId, targetShard)
	}

	// ── Step 2: coordinator assigns shard to node B ───────────────────────
	regionA.Receive(ShardHome{ShardId: req.ShardId, RegionPath: regionBPath})

	// Node A must not have spawned any local actors (the shard lives on B).
	if len(mctxA.created) != 0 {
		t.Errorf("node A must not spawn local actors for remote shards; got: %v", mctxA.created)
	}

	// ── Step 3: verify delivery on node B ────────────────────────────────
	// B should have spawned exactly one local shard actor.
	if len(mctxB.created) != 1 {
		t.Fatalf("expected 1 shard spawned on node B, got %d: %v", len(mctxB.created), mctxB.created)
	}
	shardMock := mctxB.actors[mctxB.created[0]].(*mockRef)
	if len(shardMock.messages) != 1 {
		t.Fatalf("expected 1 message delivered to B's shard, got %d", len(shardMock.messages))
	}
	env, ok := shardMock.messages[0].(ShardingEnvelope)
	if !ok {
		t.Fatalf("expected ShardingEnvelope in B's shard, got %T", shardMock.messages[0])
	}
	if env.EntityId != "entity-1" {
		t.Errorf("EntityId = %q, want %q", env.EntityId, "entity-1")
	}
}

// TestCoordinator_LeastShardsAllocation verifies that the coordinator's
// LeastShardAllocationStrategy assigns a new shard to the region that
// currently hosts the fewest shards.
//
// Setup: three regions registered; A owns 2 shards, B owns 1, C owns 0.
// Expected: the next allocation goes to C.
func TestCoordinator_LeastShardsAllocation(t *testing.T) {
	coord := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	mctx := newMockActorContext()
	actor.InjectSystem(coord, mctx)
	coord.SetSelf(&mockRef{path: "/user/coordinator"})

	regionRefs := map[string]*mockRef{
		"/user/regionA": {path: "/user/regionA"},
		"/user/regionB": {path: "/user/regionB"},
		"/user/regionC": {path: "/user/regionC"},
	}

	// Register all three regions with the coordinator.
	for path, ref := range regionRefs {
		actor.InjectSender(coord, ref)
		coord.Receive(RegisterRegion{RegionPath: path})
	}

	// Pre-assign shards so the load is uneven:
	//   regionA → 2 shards, regionB → 1 shard, regionC → 0 shards
	coord.shards["shard-0"] = "/user/regionA"
	coord.shards["shard-1"] = "/user/regionA"
	coord.shards["shard-2"] = "/user/regionB"

	// Ask coordinator for a new shard; sender is regionA (the requester).
	requester := regionRefs["/user/regionA"]
	actor.InjectSender(coord, requester)
	coord.Receive(GetShardHome{ShardId: "shard-new"})

	// The coordinator replies to the requester (regionA).
	msgs := requester.messages
	if len(msgs) == 0 {
		t.Fatal("coordinator did not reply to requester with ShardHome")
	}
	home, ok := msgs[len(msgs)-1].(ShardHome)
	if !ok {
		t.Fatalf("expected ShardHome reply, got %T", msgs[len(msgs)-1])
	}
	if home.ShardId != "shard-new" {
		t.Errorf("ShardHome.ShardId = %q, want %q", home.ShardId, "shard-new")
	}
	// regionC has 0 shards — it must be chosen by LeastShardAllocationStrategy.
	if home.RegionPath != "/user/regionC" {
		t.Errorf("ShardHome.RegionPath = %q, want %q (least-loaded region)",
			home.RegionPath, "/user/regionC")
	}
}

// TestCoordinator_RebalanceTwoNodes is the Phase 3 integration test that
// verifies the full rebalancing flow:
//
//  1. Node A joins and receives all four shards.
//  2. Node B joins with zero shards.
//  3. A rebalanceTick fires → coordinator initiates BeginHandOff for one shard.
//  4. The test simulates the region responding with BeginHandOffAck and then
//     (after receiving HandOff) with ShardStopped.
//  5. The freed shard is re-requested by region B and allocated to B.
//  6. A second rebalanceTick + handoff cycle moves a second shard to B,
//     achieving the target balanced state of 2 shards each.
func TestCoordinator_RebalanceTwoNodes(t *testing.T) {
	// maxSimultaneousRebalance=1 so each tick moves exactly one shard.
	coord := NewShardCoordinator(NewLeastShardAllocationStrategy(1, 1))
	mctx := newMockActorContext()
	actor.InjectSystem(coord, mctx)
	coord.SetSelf(&mockRef{path: "/user/coordinator"})

	regionA := &mockRef{path: "/user/regionA"}
	regionB := &mockRef{path: "/user/regionB"}

	// ── Setup: register A, allocate 4 shards ──────────────────────────────
	actor.InjectSender(coord, regionA)
	coord.Receive(RegisterRegion{RegionPath: "/user/regionA"})

	for i := 0; i < 4; i++ {
		actor.InjectSender(coord, regionA)
		coord.Receive(GetShardHome{ShardId: fmt.Sprintf("shard-%d", i)})
	}
	if len(coord.shards) != 4 {
		t.Fatalf("setup: expected 4 shards allocated to A, got %d", len(coord.shards))
	}
	for sid, rp := range coord.shards {
		if rp != "/user/regionA" {
			t.Errorf("setup: shard %s expected on A, got %s", sid, rp)
		}
	}

	// ── B joins ────────────────────────────────────────────────────────────
	actor.InjectSender(coord, regionB)
	coord.Receive(RegisterRegion{RegionPath: "/user/regionB"})

	// At this point: A=4 shards, B=0 shards → imbalance of 4 > threshold 1.

	// ── Rebalance round 1 ─────────────────────────────────────────────────
	coord.Receive(rebalanceTick{})

	// Coordinator should have sent exactly 1 BeginHandOff to A.
	var handoffShards []ShardId
	for _, msg := range regionA.messages {
		if bho, ok := msg.(BeginHandOff); ok {
			handoffShards = append(handoffShards, bho.ShardId)
		}
	}
	if len(handoffShards) != 1 {
		t.Fatalf("round 1: expected 1 BeginHandOff, got %d (regionA.messages: %v)",
			len(handoffShards), regionA.messages)
	}
	shard1 := handoffShards[0]

	// Simulate region A completing the first handoff:
	//   BeginHandOffAck → coordinator sends HandOff back → ShardStopped
	actor.InjectSender(coord, regionA)
	coord.Receive(BeginHandOffAck{ShardId: shard1})

	handOffSent := false
	for _, msg := range regionA.messages {
		if ho, ok := msg.(HandOff); ok && ho.ShardId == shard1 {
			handOffSent = true
			break
		}
	}
	if !handOffSent {
		t.Errorf("round 1: coordinator must send HandOff to A after BeginHandOffAck")
	}

	actor.InjectSender(coord, regionA)
	coord.Receive(ShardStopped{ShardId: shard1})

	if _, still := coord.shards[shard1]; still {
		t.Errorf("round 1: shard %s must be cleared after ShardStopped", shard1)
	}
	if _, inProg := coord.rebalanceInProgress[shard1]; inProg {
		t.Errorf("round 1: shard %s must leave rebalanceInProgress after ShardStopped", shard1)
	}

	// ── Rebalance round 2 ─────────────────────────────────────────────────
	// A=3 shards, B=0 shards — still imbalanced.
	coord.Receive(rebalanceTick{})

	var newHandoffShards []ShardId
	for _, msg := range regionA.messages {
		if bho, ok := msg.(BeginHandOff); ok && bho.ShardId != shard1 {
			newHandoffShards = append(newHandoffShards, bho.ShardId)
		}
	}
	if len(newHandoffShards) == 0 {
		t.Fatalf("round 2: expected a second BeginHandOff for a different shard")
	}
	shard2 := newHandoffShards[0]

	actor.InjectSender(coord, regionA)
	coord.Receive(BeginHandOffAck{ShardId: shard2})
	actor.InjectSender(coord, regionA)
	coord.Receive(ShardStopped{ShardId: shard2})

	if _, still := coord.shards[shard2]; still {
		t.Errorf("round 2: shard %s must be cleared after ShardStopped", shard2)
	}

	// ── Verify reallocation to B ──────────────────────────────────────────
	// Region B asks for the two freed shards.  With A=2 and B=0, both must go to B.
	for _, sid := range []ShardId{shard1, shard2} {
		actor.InjectSender(coord, regionB)
		coord.Receive(GetShardHome{ShardId: sid})
	}

	// Collect ShardHome replies received by B.
	allocatedToB := 0
	for _, msg := range regionB.messages {
		if sh, ok := msg.(ShardHome); ok {
			if sh.RegionPath == "/user/regionB" {
				allocatedToB++
			} else {
				t.Errorf("freed shard %s reallocated to %q instead of regionB", sh.ShardId, sh.RegionPath)
			}
		}
	}
	if allocatedToB != 2 {
		t.Errorf("expected 2 shards reallocated to regionB, got %d", allocatedToB)
	}
}

// ── Phase 1 tests (unchanged below) ──────────────────────────────────────────

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

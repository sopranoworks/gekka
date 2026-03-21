/*
 * protocol.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package sharding implements Cluster Sharding — location-transparent routing
// of messages to sharded entities across a cluster.
//
// Phase 1 Protocol Overview:
//
//   1. A client sends a message to a ShardRegion on any node.
//   2. The ShardRegion applies a ShardIdExtractor to determine the EntityID
//      and ShardID for the message.
//   3. If the ShardID's home is unknown, the ShardRegion sends GetShardHome
//      to the ShardCoordinator (a cluster singleton).
//   4. The ShardCoordinator allocates the shard using its ShardAllocationStrategy
//      and replies with ShardHome.
//   5. The ShardRegion caches the home and forwards the EntityEnvelope to the
//      owning ShardRegion (which may be itself for local shards).
//   6. The owning ShardRegion's Shard spawns the entity actor on first use and
//      delivers the unwrapped message.
package sharding

// EntityID is a stable, unique identifier for a sharded entity.
// It is a string alias so that it can be used as a map key and compared
// cheaply.  Example values: "user-42", "order-abc123".
type EntityID = EntityId

// ShardID identifies a shard bucket.  Entities are grouped into shards; each
// shard is owned by exactly one ShardRegion at a time.
// Example values: "0" through "9" for a 10-shard configuration.
type ShardID = ShardId

// ShardIdExtractor is a function that inspects an incoming message and returns:
//   - entityID: the stable identifier of the target entity,
//   - shardID:  the shard bucket this entity belongs to,
//   - msg:      the unwrapped payload that will be forwarded to the entity
//               (often the same as the input, but may be an inner message
//               when the original was wrapped in an application envelope).
//
// The extractor must be deterministic: the same entityID must always produce
// the same shardID.  A common implementation hashes the entityID modulo the
// number of shards:
//
//	func myExtractor(msg any) (sharding.EntityID, sharding.ShardID, any) {
//	    if m, ok := msg.(MyMsg); ok {
//	        h := fnv.New32a()
//	        h.Write([]byte(m.UserID))
//	        return m.UserID, fmt.Sprintf("%d", h.Sum32()%10), m
//	    }
//	    return "", "", msg
//	}
type ShardIdExtractor = ExtractEntityId

// EntityEnvelope is the standard wire envelope used internally by the sharding
// subsystem to carry a user message together with its routing metadata.
//
// Application code typically does not construct EntityEnvelope directly; it is
// created by ShardRegion after applying the ShardIdExtractor.  The only case
// where application code sees EntityEnvelope is when implementing a custom
// ShardAllocationStrategy or a passivation-aware entity.
type EntityEnvelope = ShardingEnvelope

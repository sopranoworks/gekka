/*
 * hash_extractor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"hash/fnv"
)

// HashCodeExtractor creates an ExtractEntityId function that computes the
// shard ID by hashing the entity ID modulo numberOfShards.  This mirrors
// Pekko's HashCodeMessageExtractor which uses
//
//	Math.abs(entityId.hashCode()) % numberOfShards
//
// The extractEntity function extracts the entity ID and unwrapped payload
// from the incoming message.  It should return ("", nil) for unrecognised
// messages.
//
// Example:
//
//	extract := sharding.HashCodeExtractor(100, func(msg any) (string, any) {
//	    if m, ok := msg.(MyMsg); ok {
//	        return m.UserID, m
//	    }
//	    return "", nil
//	})
func HashCodeExtractor(numberOfShards int, extractEntity func(msg any) (EntityId, any)) ExtractEntityId {
	if numberOfShards <= 0 {
		numberOfShards = 1000 // Pekko default
	}
	return func(msg any) (EntityId, ShardId, any) {
		entityId, payload := extractEntity(msg)
		if entityId == "" {
			return "", "", msg
		}
		h := fnv.New32a()
		h.Write([]byte(entityId))
		shardId := fmt.Sprintf("%d", h.Sum32()%uint32(numberOfShards))
		return entityId, shardId, payload
	}
}

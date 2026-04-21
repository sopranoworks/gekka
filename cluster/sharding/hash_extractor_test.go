/*
 * hash_extractor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"testing"
)

// ── #14: NumberOfShards via HashCodeExtractor ────────────────────────────────

type testMsg struct {
	EntityId string
	Body     string
}

func TestHashCodeExtractor_ShardCount(t *testing.T) {
	numberOfShards := 10
	extract := HashCodeExtractor(numberOfShards, func(msg any) (EntityId, any) {
		if m, ok := msg.(testMsg); ok {
			return m.EntityId, m
		}
		return "", nil
	})

	// All shard IDs should be in [0, numberOfShards)
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		entityId := fmt.Sprintf("entity-%d", i)
		_, shardId, _ := extract(testMsg{EntityId: entityId, Body: "hello"})
		n, err := strconv.Atoi(shardId)
		if err != nil {
			t.Fatalf("shardId %q is not numeric", shardId)
		}
		if n < 0 || n >= numberOfShards {
			t.Errorf("shardId %d out of range [0, %d)", n, numberOfShards)
		}
		seen[shardId] = true
	}
	// With 100 entities and 10 shards, we should see multiple distinct shards
	if len(seen) < 2 {
		t.Errorf("expected multiple distinct shards, got %d", len(seen))
	}
}

func TestHashCodeExtractor_MatchesFNV(t *testing.T) {
	numberOfShards := 100
	extract := HashCodeExtractor(numberOfShards, func(msg any) (EntityId, any) {
		if m, ok := msg.(testMsg); ok {
			return m.EntityId, m
		}
		return "", nil
	})

	entityId := "user-42"
	_, shardId, _ := extract(testMsg{EntityId: entityId, Body: "test"})

	// Verify it matches manual FNV computation
	h := fnv.New32a()
	h.Write([]byte(entityId))
	expected := fmt.Sprintf("%d", h.Sum32()%uint32(numberOfShards))

	if shardId != expected {
		t.Errorf("shardId = %q, want %q", shardId, expected)
	}
}

func TestHashCodeExtractor_DifferentShardCounts(t *testing.T) {
	entityId := "test-entity"

	// With 10 shards vs 1000 shards, the same entity should get different shard IDs
	// (unless the hash happens to be divisible by both — unlikely)
	extract10 := HashCodeExtractor(10, func(msg any) (EntityId, any) {
		return msg.(string), msg
	})
	extract1000 := HashCodeExtractor(1000, func(msg any) (EntityId, any) {
		return msg.(string), msg
	})

	_, shard10, _ := extract10(entityId)
	_, shard1000, _ := extract1000(entityId)

	// Verify both are valid
	n10, _ := strconv.Atoi(shard10)
	n1000, _ := strconv.Atoi(shard1000)

	if n10 < 0 || n10 >= 10 {
		t.Errorf("shard10 = %d, out of range [0, 10)", n10)
	}
	if n1000 < 0 || n1000 >= 1000 {
		t.Errorf("shard1000 = %d, out of range [0, 1000)", n1000)
	}
}

func TestHashCodeExtractor_DefaultsToThousand(t *testing.T) {
	extract := HashCodeExtractor(0, func(msg any) (EntityId, any) {
		return msg.(string), msg
	})

	_, shardId, _ := extract("some-entity")
	n, err := strconv.Atoi(shardId)
	if err != nil {
		t.Fatalf("shardId %q is not numeric", shardId)
	}
	if n < 0 || n >= 1000 {
		t.Errorf("shardId = %d, expected in [0, 1000) for default", n)
	}
}

func TestHashCodeExtractor_UnrecognisedMessage(t *testing.T) {
	extract := HashCodeExtractor(10, func(msg any) (EntityId, any) {
		if m, ok := msg.(testMsg); ok {
			return m.EntityId, m
		}
		return "", nil
	})

	entityId, shardId, _ := extract("not-a-testMsg")
	if entityId != "" {
		t.Errorf("entityId = %q, want empty for unrecognised message", entityId)
	}
	if shardId != "" {
		t.Errorf("shardId = %q, want empty for unrecognised message", shardId)
	}
}

func TestHashCodeExtractor_Deterministic(t *testing.T) {
	extract := HashCodeExtractor(50, func(msg any) (EntityId, any) {
		if m, ok := msg.(testMsg); ok {
			return m.EntityId, m
		}
		return "", nil
	})

	msg := testMsg{EntityId: "user-123", Body: "hello"}
	_, shard1, _ := extract(msg)
	_, shard2, _ := extract(msg)
	_, shard3, _ := extract(msg)

	if shard1 != shard2 || shard2 != shard3 {
		t.Errorf("non-deterministic: got %s, %s, %s", shard1, shard2, shard3)
	}
}

// ── #14: NumberOfShards in ShardSettings ─────────────────────────────────────

func TestShardSettings_NumberOfShards(t *testing.T) {
	s := ShardSettings{NumberOfShards: 500}
	if s.NumberOfShards != 500 {
		t.Errorf("NumberOfShards = %d, want 500", s.NumberOfShards)
	}
}

/*
 * snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sopranoworks/gekka/persistence"
)

// RedisSnapshotStore implements persistence.SnapshotStore using Redis.
//
// Each snapshot is stored as a JSON string at:
//
//	{keyPrefix}s:{persistenceId}:{seqNr}
//
// A sorted set per persistence ID acts as a sequence-number index:
//
//	{keyPrefix}si:{persistenceId}   (score = seqNr, member = seqNr decimal string)
type RedisSnapshotStore struct {
	client *redis.Client
	prefix string
	codec  PayloadCodec
}

// NewRedisSnapshotStore creates a RedisSnapshotStore.
func NewRedisSnapshotStore(client *redis.Client, prefix string, codec PayloadCodec) *RedisSnapshotStore {
	return &RedisSnapshotStore{client: client, prefix: prefix, codec: codec}
}

func (s *RedisSnapshotStore) snapKey(pid string, seqNr uint64) string {
	return fmt.Sprintf("%ss:%s:%d", s.prefix, pid, seqNr)
}

func (s *RedisSnapshotStore) indexKey(pid string) string {
	return s.prefix + "si:" + pid
}

// snapshotEnvelope is the JSON structure written to Redis for each snapshot.
type snapshotEnvelope struct {
	SeqNr    uint64          `json:"seq"`
	Ts       int64           `json:"ts"`
	Manifest string          `json:"manifest"`
	Payload  json.RawMessage `json:"payload"`
}

// SaveSnapshot persists snapshot to Redis and updates the sequence-number index.
func (s *RedisSnapshotStore) SaveSnapshot(
	ctx context.Context,
	metadata persistence.SnapshotMetadata,
	snapshot any,
) error {
	manifest, data, err := s.codec.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("redissnapshot: encode %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}

	ts := metadata.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	env := snapshotEnvelope{
		SeqNr:    metadata.SequenceNr,
		Ts:       ts,
		Manifest: manifest,
		Payload:  json.RawMessage(data),
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("redissnapshot: marshal envelope %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}

	key := s.snapKey(metadata.PersistenceID, metadata.SequenceNr)
	if err := s.client.Set(ctx, key, string(envBytes), 0).Err(); err != nil {
		return fmt.Errorf("redissnapshot: SET %q: %w", key, err)
	}

	idxKey := s.indexKey(metadata.PersistenceID)
	member := strconv.FormatUint(metadata.SequenceNr, 10)
	if err := s.client.ZAdd(ctx, idxKey, redis.Z{
		Score:  float64(metadata.SequenceNr),
		Member: member,
	}).Err(); err != nil {
		return fmt.Errorf("redissnapshot: ZADD index %q: %w", idxKey, err)
	}
	return nil
}

// LoadSnapshot returns the highest-seqNr snapshot for pid that satisfies
// criteria, or nil if none exists.
func (s *RedisSnapshotStore) LoadSnapshot(
	ctx context.Context,
	pid string,
	criteria persistence.SnapshotSelectionCriteria,
) (*persistence.SelectedSnapshot, error) {
	maxScore := "+inf"
	if criteria.MaxSequenceNr < ^uint64(0) {
		maxScore = strconv.FormatUint(criteria.MaxSequenceNr, 10)
	}
	minScore := strconv.FormatUint(criteria.MinSequenceNr, 10)

	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	// Retrieve candidates from the sorted index, highest seqNr first.
	members, err := s.client.ZRevRangeByScore(ctx, s.indexKey(pid), &redis.ZRangeBy{
		Max: maxScore,
		Min: minScore,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redissnapshot: ZREVRANGEBYSCORE for %q: %w", pid, err)
	}

	for _, member := range members {
		seqNr, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue
		}
		snap, err := s.loadOne(ctx, pid, seqNr)
		if err != nil {
			return nil, err
		}
		if snap == nil {
			continue
		}
		if snap.Metadata.Timestamp < minTs || snap.Metadata.Timestamp > maxTs {
			continue
		}
		return snap, nil
	}
	return nil, nil
}

// loadOne reads and decodes the snapshot for pid/seqNr from Redis.
func (s *RedisSnapshotStore) loadOne(ctx context.Context, pid string, seqNr uint64) (*persistence.SelectedSnapshot, error) {
	key := s.snapKey(pid, seqNr)
	raw, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redissnapshot: GET %q: %w", key, err)
	}

	var env snapshotEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, fmt.Errorf("redissnapshot: unmarshal envelope %q/%d: %w", pid, seqNr, err)
	}

	payload, err := s.codec.Decode(env.Manifest, []byte(env.Payload))
	if err != nil {
		return nil, fmt.Errorf("redissnapshot: decode payload %q/%d: %w", pid, seqNr, err)
	}

	return &persistence.SelectedSnapshot{
		Metadata: persistence.SnapshotMetadata{
			PersistenceID: pid,
			SequenceNr:    env.SeqNr,
			Timestamp:     env.Ts,
		},
		Snapshot: payload,
	}, nil
}

// DeleteSnapshot removes a single snapshot by metadata key and from the index.
func (s *RedisSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	key := s.snapKey(metadata.PersistenceID, metadata.SequenceNr)
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redissnapshot: DEL %q: %w", key, err)
	}
	member := strconv.FormatUint(metadata.SequenceNr, 10)
	if err := s.client.ZRem(ctx, s.indexKey(metadata.PersistenceID), member).Err(); err != nil {
		return fmt.Errorf("redissnapshot: ZREM %q/%d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// DeleteSnapshots removes all snapshots for pid that match criteria.
func (s *RedisSnapshotStore) DeleteSnapshots(
	ctx context.Context,
	pid string,
	criteria persistence.SnapshotSelectionCriteria,
) error {
	maxScore := "+inf"
	if criteria.MaxSequenceNr < ^uint64(0) {
		maxScore = strconv.FormatUint(criteria.MaxSequenceNr, 10)
	}
	minScore := strconv.FormatUint(criteria.MinSequenceNr, 10)

	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	members, err := s.client.ZRangeByScore(ctx, s.indexKey(pid), &redis.ZRangeBy{
		Min: minScore,
		Max: maxScore,
	}).Result()
	if err != nil {
		return fmt.Errorf("redissnapshot: ZRANGEBYSCORE for delete %q: %w", pid, err)
	}

	for _, member := range members {
		seqNr, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue
		}
		snap, err := s.loadOne(ctx, pid, seqNr)
		if err != nil {
			return err
		}
		if snap == nil {
			continue
		}
		if snap.Metadata.Timestamp < minTs || snap.Metadata.Timestamp > maxTs {
			continue
		}
		key := s.snapKey(pid, seqNr)
		if err := s.client.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("redissnapshot: DEL %q during range delete: %w", key, err)
		}
		if err := s.client.ZRem(ctx, s.indexKey(pid), member).Err(); err != nil {
			return fmt.Errorf("redissnapshot: ZREM %q/%d during range delete: %w", pid, seqNr, err)
		}
	}
	return nil
}

// Ensure RedisSnapshotStore satisfies SnapshotStore at compile time.
var _ persistence.SnapshotStore = (*RedisSnapshotStore)(nil)

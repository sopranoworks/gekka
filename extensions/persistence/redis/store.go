/*
 * store.go
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

	"github.com/redis/go-redis/v9"
	"github.com/sopranoworks/gekka/persistence"
)

// RedisDurableStateStore implements persistence.DurableStateStore using Redis.
//
// Each actor's current state is stored as a single JSON value at:
//
//	{keyPrefix}d:{persistenceId}
//
// The value is a JSON envelope containing the revision, tag, type manifest,
// and the encoded payload.
type RedisDurableStateStore struct {
	client *redis.Client
	prefix string
	codec  PayloadCodec
}

// NewRedisDurableStateStore creates a RedisDurableStateStore.
func NewRedisDurableStateStore(client *redis.Client, prefix string, codec PayloadCodec) *RedisDurableStateStore {
	return &RedisDurableStateStore{client: client, prefix: prefix, codec: codec}
}

func (s *RedisDurableStateStore) stateKey(pid string) string {
	return s.prefix + "d:" + pid
}

// durableStateEnvelope is the JSON structure written to Redis for each state entry.
type durableStateEnvelope struct {
	Revision uint64          `json:"revision"`
	Tag      string          `json:"tag"`
	Manifest string          `json:"manifest"`
	Payload  json.RawMessage `json:"payload"`
}

// Get retrieves the current state and revision for persistenceID.
// Returns (nil, 0, nil) if no state has been stored yet.
func (s *RedisDurableStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	key := s.stateKey(persistenceID)
	raw, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("redisstate: GET %q: %w", key, err)
	}

	var env durableStateEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, 0, fmt.Errorf("redisstate: unmarshal envelope %q: %w", persistenceID, err)
	}

	payload, err := s.codec.Decode(env.Manifest, []byte(env.Payload))
	if err != nil {
		return nil, 0, fmt.Errorf("redisstate: decode payload %q: %w", persistenceID, err)
	}

	return payload, env.Revision, nil
}

// Upsert stores state as the current durable state for persistenceID at revision seqNr.
func (s *RedisDurableStateStore) Upsert(ctx context.Context, persistenceID string, seqNr uint64, state any, tag string) error {
	manifest, data, err := s.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("redisstate: encode %q: %w", persistenceID, err)
	}

	env := durableStateEnvelope{
		Revision: seqNr,
		Tag:      tag,
		Manifest: manifest,
		Payload:  json.RawMessage(data),
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("redisstate: marshal envelope %q: %w", persistenceID, err)
	}

	key := s.stateKey(persistenceID)
	if err := s.client.Set(ctx, key, string(envBytes), 0).Err(); err != nil {
		return fmt.Errorf("redisstate: SET %q: %w", key, err)
	}
	return nil
}

// Delete removes the durable state for persistenceID.
func (s *RedisDurableStateStore) Delete(ctx context.Context, persistenceID string) error {
	key := s.stateKey(persistenceID)
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redisstate: DEL %q: %w", key, err)
	}
	return nil
}

// Ensure RedisDurableStateStore satisfies DurableStateStore at compile time.
var _ persistence.DurableStateStore = (*RedisDurableStateStore)(nil)

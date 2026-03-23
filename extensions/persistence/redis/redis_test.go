/*
 * redis_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package redisstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sopranoworks/gekka/persistence"
	redisstore "github.com/sopranoworks/gekka-extensions-persistence-redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisAddr returns the Redis address from the REDIS_ADDR environment variable.
// Tests are skipped when the variable is not set so CI does not require Redis.
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set — skipping Redis integration tests")
	}
	return addr
}

// newClient creates a test Redis client and registers a cleanup function.
func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniquePrefix returns a test-scoped key prefix to prevent inter-test
// collisions even when Redis data is not cleaned between runs.
func uniquePrefix(t *testing.T) string {
	return "test:" + t.Name() + ":"
}

// ── Test types ────────────────────────────────────────────────────────────────

type OrderPlaced struct{ Item string }
type OrderShipped struct{ TrackingID string }
type CartState struct{ Items []string }

// ── Journal tests ─────────────────────────────────────────────────────────────

func TestRedis_Journal_WriteAndReplay(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "order-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "banana"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "cherry"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), highest)

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 3)
	assert.Equal(t, uint64(1), replayed[0].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "apple"}, replayed[0].Payload)
	assert.Equal(t, OrderPlaced{Item: "banana"}, replayed[1].Payload)
	assert.Equal(t, OrderPlaced{Item: "cherry"}, replayed[2].Payload)
}

func TestRedis_Journal_ReplayRange(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "range-order"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "a"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "b"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "c"}},
		{PersistenceID: pid, SequenceNr: 4, Payload: OrderPlaced{Item: "d"}},
		{PersistenceID: pid, SequenceNr: 5, Payload: OrderPlaced{Item: "e"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	// Replay only seq 2–4.
	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 2, 4, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 3)
	assert.Equal(t, uint64(2), replayed[0].SequenceNr)
	assert.Equal(t, uint64(4), replayed[2].SequenceNr)
}

func TestRedis_Journal_MaxLimit(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "limited-order"

	var events []persistence.PersistentRepr
	for i := uint64(1); i <= 10; i++ {
		events = append(events, persistence.PersistentRepr{
			PersistenceID: pid, SequenceNr: i,
			Payload: OrderPlaced{Item: "item"},
		})
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	var count int
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 10, 3, func(_ persistence.PersistentRepr) {
		count++
	}))
	assert.Equal(t, 3, count)
}

func TestRedis_Journal_Delete(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "delete-order"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "a"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "b"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "c"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	// Delete up to seq 2.
	require.NoError(t, j.AsyncDeleteMessagesTo(ctx, pid, 2))

	// Only seq 3 should remain.
	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 1)
	assert.Equal(t, uint64(3), replayed[0].SequenceNr)
}

func TestRedis_Journal_ReadHighestSequenceNr_Empty(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	j := redisstore.NewRedisJournal(client, uniquePrefix(t), codec)
	ctx := context.Background()

	highest, err := j.ReadHighestSequenceNr(ctx, "nonexistent-pid", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), highest)
}

// ── SnapshotStore tests ───────────────────────────────────────────────────────

func TestRedis_SnapshotStore_SaveAndLoad(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "cart-1"

	ts := time.Now().UnixNano()
	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5, Timestamp: ts}
	require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{"a", "b"}}))

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(5), snap.Metadata.SequenceNr)
	state, ok := snap.Snapshot.(CartState)
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b"}, state.Items)
}

func TestRedis_SnapshotStore_LoadLatest(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "cart-latest"

	for _, seqNr := range []uint64{3, 7, 5} {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: seqNr}
		require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{
			"seq-" + string(rune('0'+seqNr)),
		}}))
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	// Should return seq 7 (highest).
	assert.Equal(t, uint64(7), snap.Metadata.SequenceNr)
}

func TestRedis_SnapshotStore_NotFound(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	ss := redisstore.NewRedisSnapshotStore(client, uniquePrefix(t), codec)
	ctx := context.Background()

	snap, err := ss.LoadSnapshot(ctx, "nonexistent-actor", persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestRedis_SnapshotStore_Delete(t *testing.T) {
	addr := redisAddr(t)
	client := newClient(t, addr)

	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(client, uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "cart-delete"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 10}
	require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{"x"}}))

	require.NoError(t, ss.DeleteSnapshot(ctx, meta))

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

// ── Registration smoke-test ───────────────────────────────────────────────────

func TestRedis_Registration(t *testing.T) {
	addr := redisAddr(t)

	// Verify that the init() function registered both providers.
	// We do this by attempting to create providers via the registry.
	cfgStr := `{ address: "` + addr + `", key-prefix: "regtest:" }`
	cfg, err := redisstore.ParseConfigString(cfgStr)
	require.NoError(t, err)

	j, err := persistence.NewJournal("redis", *cfg)
	require.NoError(t, err, "redis journal provider should be registered after blank import")
	assert.NotNil(t, j)

	ss, err := persistence.NewSnapshotStore("redis", *cfg)
	require.NoError(t, err, "redis snapshot store provider should be registered after blank import")
	assert.NotNil(t, ss)
}

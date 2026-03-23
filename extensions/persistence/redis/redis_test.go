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
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sopranoworks/gekka/persistence"
	redisstore "github.com/sopranoworks/gekka-extensions-persistence-redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// testRedisAddr is set by TestMain to the host:port of the container.
var testRedisAddr string

// TestMain starts a single Redis container for the entire test binary, runs
// all tests, then terminates the container.  If Docker is not reachable on
// the host the test binary exits with code 0 (all skipped).
func TestMain(m *testing.M) {
	ctx := context.Background()

	// ── Docker availability probe ─────────────────────────────────────────────
	// Attempt to reach the Docker daemon before pulling any image.  This avoids
	// a confusing timeout when Docker Desktop is simply not running.
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Docker not found, skipping Redis integration tests.")
		os.Exit(0)
	}
	if _, err := cli.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "Docker not found, skipping Redis integration tests.")
		os.Exit(0)
	}
	cli.Close()

	// ── Start Redis container ─────────────────────────────────────────────────
	ctr, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start Redis container: %v\n", err)
		os.Exit(1)
	}

	// ── Resolve connection address ────────────────────────────────────────────
	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		fmt.Fprintf(os.Stderr, "Redis container: get connection string: %v\n", err)
		os.Exit(1)
	}
	// connStr is "redis://host:port" — parse it to extract "host:port".
	opts, err := redis.ParseURL(connStr)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		fmt.Fprintf(os.Stderr, "Redis container: parse URL %q: %v\n", connStr, err)
		os.Exit(1)
	}
	testRedisAddr = opts.Addr

	// ── Run tests ─────────────────────────────────────────────────────────────
	code := m.Run()

	// ── Teardown ──────────────────────────────────────────────────────────────
	if err := testcontainers.TerminateContainer(ctr); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate Redis container: %v\n", err)
	}

	os.Exit(code)
}

// newClient creates a test Redis client connected to the container.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniquePrefix returns a test-scoped key prefix so parallel or sequential
// tests never collide even if the same Redis instance is reused.
func uniquePrefix(t *testing.T) string {
	return "test:" + t.Name() + ":"
}

// ── Test types ────────────────────────────────────────────────────────────────

type OrderPlaced struct{ Item string }
type CartState struct{ Items []string }

// ── Journal tests ─────────────────────────────────────────────────────────────

func TestRedis_Journal_WriteAndReplay(t *testing.T) {
	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(newClient(t), uniquePrefix(t), codec)
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
	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(newClient(t), uniquePrefix(t), codec)
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
	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(newClient(t), uniquePrefix(t), codec)
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
	codec := redisstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := redisstore.NewRedisJournal(newClient(t), uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "delete-order"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "a"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "b"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "c"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	require.NoError(t, j.AsyncDeleteMessagesTo(ctx, pid, 2))

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 1)
	assert.Equal(t, uint64(3), replayed[0].SequenceNr)
}

func TestRedis_Journal_ReadHighestSequenceNr_Empty(t *testing.T) {
	codec := redisstore.NewJSONCodec()
	j := redisstore.NewRedisJournal(newClient(t), uniquePrefix(t), codec)
	ctx := context.Background()

	highest, err := j.ReadHighestSequenceNr(ctx, "nonexistent-pid", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), highest)
}

// ── SnapshotStore tests ───────────────────────────────────────────────────────

func TestRedis_SnapshotStore_SaveAndLoad(t *testing.T) {
	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(newClient(t), uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "cart-1"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
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
	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(newClient(t), uniquePrefix(t), codec)
	ctx := context.Background()
	const pid = "cart-latest"

	for _, seqNr := range []uint64{3, 7, 5} {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: seqNr}
		require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{fmt.Sprintf("seq-%d", seqNr)}}))
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	// Should return seqNr 7 (highest stored).
	assert.Equal(t, uint64(7), snap.Metadata.SequenceNr)
}

func TestRedis_SnapshotStore_NotFound(t *testing.T) {
	codec := redisstore.NewJSONCodec()
	ss := redisstore.NewRedisSnapshotStore(newClient(t), uniquePrefix(t), codec)
	ctx := context.Background()

	snap, err := ss.LoadSnapshot(ctx, "nonexistent-actor", persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestRedis_SnapshotStore_Delete(t *testing.T) {
	codec := redisstore.NewJSONCodec()
	codec.Register(CartState{})

	ss := redisstore.NewRedisSnapshotStore(newClient(t), uniquePrefix(t), codec)
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
	// Verify that the init() function registered both providers by creating
	// instances through the persistence registry with the live container address.
	cfgStr := fmt.Sprintf("address = \"%s\"\nkey-prefix = \"regtest:\"", testRedisAddr)
	cfg, err := redisstore.ParseConfigString(cfgStr)
	require.NoError(t, err)

	j, err := persistence.NewJournal("redis", *cfg)
	require.NoError(t, err, "redis journal provider should be registered after blank import")
	assert.NotNil(t, j)

	ss, err := persistence.NewSnapshotStore("redis", *cfg)
	require.NoError(t, err, "redis snapshot store provider should be registered after blank import")
	assert.NotNil(t, ss)
}

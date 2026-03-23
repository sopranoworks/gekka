/*
 * persistence_redis_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package bench

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	redisstore "github.com/sopranoworks/gekka-extensions-persistence-redis"
	"github.com/sopranoworks/gekka/persistence"
	spannerstore "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// ── Redis container lifecycle (shared across all benchmarks) ──────────────────

var (
	redisOnce      sync.Once
	redisBenchAddr string // empty → Docker unavailable; benchmarks skip
	redisBenchCtr  *tcredis.RedisContainer
)

// initRedisBench starts a single Redis container for the entire benchmark
// binary.  It is called via sync.Once so the container is started at most
// once regardless of how many benchmark functions run.
func initRedisBench() {
	ctx := context.Background()

	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return
	}
	if _, err := cli.Ping(ctx); err != nil {
		cli.Close()
		return
	}
	cli.Close()

	ctr, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return
	}

	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return
	}
	opts, err := redis.ParseURL(connStr)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return
	}

	redisBenchAddr = opts.Addr
	redisBenchCtr = ctr
}

// setupRedis returns a connected *redis.Client and a key-prefix scoped to b.
// If Docker is unavailable the benchmark is skipped.
func setupRedis(b *testing.B) (*redis.Client, string) {
	b.Helper()
	redisOnce.Do(initRedisBench)
	if redisBenchAddr == "" {
		b.Skip("Docker not available; skipping Redis benchmark")
	}
	client := redis.NewClient(&redis.Options{Addr: redisBenchAddr})
	b.Cleanup(func() { _ = client.Close() })
	prefix := fmt.Sprintf("bench:%s:", b.Name())
	return client, prefix
}

// ── Shared benchmark payload types (same as persistence_test.go) ─────────────

// benchWriteMsgs builds a slice of n PersistentRepr for pid starting at seq 1.
func benchWriteMsgs(pid string, n int) []persistence.PersistentRepr {
	msgs := make([]persistence.PersistentRepr, n)
	for i := 0; i < n; i++ {
		msgs[i] = persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    uint64(i + 1),
			Payload:       BenchEvent{V: 1},
		}
	}
	return msgs
}

// ── BenchmarkRedisJournal ─────────────────────────────────────────────────────

// BenchmarkRedisJournal measures Journal write and replay latency for Redis and
// Spanner side-by-side so results are directly comparable.
//
// Parameters match the existing BenchmarkRecoveryPerformance constants:
//   - 10 events per write call (= recoveryEventCount / 10 batches of 10)
//   - Replay over the same 10 events
func BenchmarkRedisJournal(b *testing.B) {
	const pid = "bench-journal-pid"
	const eventsPerOp = 10

	b.Run("Redis/Write", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		j := redisstore.NewRedisJournal(client, prefix, codec)
		ctx := context.Background()
		msgs := benchWriteMsgs(pid, eventsPerOp)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Advance sequence numbers so each iteration writes fresh entries.
			for k := range msgs {
				msgs[k].SequenceNr = uint64(i*eventsPerOp + k + 1)
			}
			if err := j.AsyncWriteMessages(ctx, msgs); err != nil {
				b.Fatalf("AsyncWriteMessages: %v", err)
			}
		}
	})

	b.Run("Redis/Replay", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		j := redisstore.NewRedisJournal(client, prefix, codec)
		ctx := context.Background()

		// Pre-populate outside the measured loop.
		if err := j.AsyncWriteMessages(ctx, benchWriteMsgs(pid, eventsPerOp)); err != nil {
			b.Fatalf("pre-populate: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := j.ReplayMessages(ctx, pid, 1, uint64(eventsPerOp), 0, func(_ persistence.PersistentRepr) {}); err != nil {
				b.Fatalf("ReplayMessages: %v", err)
			}
		}
	})

	b.Run("Spanner/Write", func(b *testing.B) {
		client, cleanup := setupSpanner(b)
		defer cleanup()

		codec := spannerstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		j := spannerstore.NewSpannerJournal(client, codec)
		ctx := context.Background()
		msgs := benchWriteMsgs(pid, eventsPerOp)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for k := range msgs {
				msgs[k].SequenceNr = uint64(i*eventsPerOp + k + 1)
			}
			if err := j.AsyncWriteMessages(ctx, msgs); err != nil {
				b.Fatalf("AsyncWriteMessages: %v", err)
			}
		}
	})

	b.Run("Spanner/Replay", func(b *testing.B) {
		client, cleanup := setupSpanner(b)
		defer cleanup()

		codec := spannerstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		j := spannerstore.NewSpannerJournal(client, codec)
		ctx := context.Background()

		if err := j.AsyncWriteMessages(ctx, benchWriteMsgs(pid, eventsPerOp)); err != nil {
			b.Fatalf("pre-populate: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := j.ReplayMessages(ctx, pid, 1, uint64(eventsPerOp), 0, func(_ persistence.PersistentRepr) {}); err != nil {
				b.Fatalf("ReplayMessages: %v", err)
			}
		}
	})
}

// ── BenchmarkRedisSnapshotStore ───────────────────────────────────────────────

// BenchmarkRedisSnapshotStore measures snapshot save latency for Redis and Spanner.
func BenchmarkRedisSnapshotStore(b *testing.B) {
	const pid = "bench-snapshot-pid"
	state := BenchState{Total: recoveryEventCount}

	b.Run("Redis/Save", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchState{})
		ss := redisstore.NewRedisSnapshotStore(client, prefix, codec)
		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			meta := persistence.SnapshotMetadata{
				PersistenceID: pid,
				SequenceNr:    uint64(i + 1),
			}
			if err := ss.SaveSnapshot(ctx, meta, state); err != nil {
				b.Fatalf("SaveSnapshot: %v", err)
			}
		}
	})

	b.Run("Redis/Load", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchState{})
		ss := redisstore.NewRedisSnapshotStore(client, prefix, codec)
		ctx := context.Background()

		// Pre-populate outside the loop.
		if err := ss.SaveSnapshot(ctx, persistence.SnapshotMetadata{
			PersistenceID: pid, SequenceNr: 1,
		}, state); err != nil {
			b.Fatalf("pre-populate: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria()); err != nil {
				b.Fatalf("LoadSnapshot: %v", err)
			}
		}
	})

	b.Run("Spanner/Save", func(b *testing.B) {
		client, cleanup := setupSpanner(b)
		defer cleanup()

		codec := spannerstore.NewJSONCodec()
		codec.Register(BenchState{})
		ss := spannerstore.NewSpannerSnapshotStore(client, codec)
		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			meta := persistence.SnapshotMetadata{
				PersistenceID: pid,
				SequenceNr:    uint64(i + 1),
			}
			if err := ss.SaveSnapshot(ctx, meta, state); err != nil {
				b.Fatalf("SaveSnapshot: %v", err)
			}
		}
	})

	b.Run("Spanner/Load", func(b *testing.B) {
		client, cleanup := setupSpanner(b)
		defer cleanup()

		codec := spannerstore.NewJSONCodec()
		codec.Register(BenchState{})
		ss := spannerstore.NewSpannerSnapshotStore(client, codec)
		ctx := context.Background()

		if err := ss.SaveSnapshot(ctx, persistence.SnapshotMetadata{
			PersistenceID: pid, SequenceNr: 1,
		}, state); err != nil {
			b.Fatalf("pre-populate: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria()); err != nil {
				b.Fatalf("LoadSnapshot: %v", err)
			}
		}
	})
}

// ── BenchmarkRedisDurableStateStore ──────────────────────────────────────────

// BenchmarkRedisDurableStateStore measures DurableStateStore upsert and get
// latency for Redis.  No Spanner equivalent exists in the current extension set.
func BenchmarkRedisDurableStateStore(b *testing.B) {
	const pid = "bench-state-pid"
	state := BenchState{Total: recoveryEventCount}

	b.Run("Redis/Upsert", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchState{})
		ds := redisstore.NewRedisDurableStateStore(client, prefix, codec)
		ctx := context.Background()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := ds.Upsert(ctx, pid, uint64(i+1), state, ""); err != nil {
				b.Fatalf("Upsert: %v", err)
			}
		}
	})

	b.Run("Redis/Get", func(b *testing.B) {
		client, prefix := setupRedis(b)
		codec := redisstore.NewJSONCodec()
		codec.Register(BenchState{})
		ds := redisstore.NewRedisDurableStateStore(client, prefix, codec)
		ctx := context.Background()

		if err := ds.Upsert(ctx, pid, 1, state, ""); err != nil {
			b.Fatalf("pre-populate: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := ds.Get(ctx, pid); err != nil {
				b.Fatalf("Get: %v", err)
			}
		}
	})
}

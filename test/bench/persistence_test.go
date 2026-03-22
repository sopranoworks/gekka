/*
 * persistence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package bench

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	spannerstore "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	sqlstore "github.com/sopranoworks/gekka/persistence/sql"
	tc "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	)
// ── Benchmark configuration ──────────────────────────────────────────────────

const (
	recoveryActorCount = 10  // Safe for emulator
	recoveryEventCount = 100 // Safe for emulator
)

type BenchEvent struct {
	V int
}

type BenchState struct {
	Total int
}

type BenchCommand struct {
	ReplyTo typed.TypedActorRef[BenchPong]
}

type BenchPong struct{}

func benchBehavior(id string, journal persistence.Journal, snapshotStore persistence.SnapshotStore, snapshotInterval uint64) *gekka.EventSourcedBehavior[BenchCommand, BenchEvent, BenchState] {
	return &gekka.EventSourcedBehavior[BenchCommand, BenchEvent, BenchState]{
		PersistenceID: id,
		Journal:       journal,
		SnapshotStore: snapshotStore,
		InitialState:  BenchState{},
		CommandHandler: func(ctx typed.TypedContext[BenchCommand], state BenchState, cmd BenchCommand) gekka.Effect[BenchEvent, BenchState] {
			cmd.ReplyTo.Tell(BenchPong{})
			return gekka.None[BenchEvent, BenchState]()
		},
		EventHandler: func(state BenchState, event BenchEvent) BenchState {
			state.Total += event.V
			return state
		},
		SnapshotInterval: snapshotInterval,
	}
}

// ── Infrastructure Helpers ───────────────────────────────────────────────────

func setupSpanner(b *testing.B) (*spanner.Client, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        "gcr.io/cloud-spanner-emulator/emulator:latest",
			ExposedPorts: []string{"9010/tcp"},
			WaitingFor:   wait.ForListeningPort("9010/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		b.Fatalf("start spanner emulator: %v", err)
	}

	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "9010/tcp")
	endpoint := fmt.Sprintf("%s:%s", host, port.Port())

	// CRITICAL: Set the emulator host environment variable BEFORE initializing clients.
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// Create Instance
	instAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		b.Fatalf("instance admin client: %v", err)
	}
	defer instAdmin.Close()
	instOp, err := instAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/test-project",
		InstanceId: "test-instance",
		Instance: &instancepb.Instance{
			Config:      "projects/test-project/instanceConfigs/emulator-config",
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		b.Fatalf("create instance: %v", err)
	}
	if _, err := instOp.Wait(ctx); err != nil {
		b.Fatalf("wait for instance: %v", err)
	}

	// Create DB
	dbAdmin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		b.Fatalf("database admin client: %v", err)
	}
	defer dbAdmin.Close()
	dbName := "projects/test-project/instances/test-instance/databases/test-db"
	dbOp, err := dbAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/test-project/instances/test-instance",
		CreateStatement: "CREATE DATABASE `test-db`",
		ExtraStatements: []string{
			spannerstore.JournalDDL,
			spannerstore.SnapshotsDDL,
		},
	})
	if err != nil {
		b.Fatalf("create database: %v", err)
	}
	if _, err := dbOp.Wait(ctx); err != nil {
		b.Fatalf("wait for database: %v", err)
	}

	client, err := spanner.NewClient(ctx, dbName, opts...)
	if err != nil {
		b.Fatalf("spanner client: %v", err)
	}

	// Readiness Check: Ping the emulator with a dummy query
	iter := client.Single().Query(ctx, spanner.NewStatement("SELECT 1"))
	defer iter.Stop()
	_, err = iter.Next()
	if err != nil && err != iterator.Done {
		b.Fatalf("readiness check failed: %v", err)
	}

	cleanup := func() {
		client.Close()
		_ = ctr.Terminate(context.Background())
		os.Unsetenv("SPANNER_EMULATOR_HOST")
	}
	return client, cleanup
}

func setupPostgres(b *testing.B) (*sql.DB, func()) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("gekka"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tc.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		b.Fatalf("start postgres: %v", err)
	}

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("postgres connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		b.Fatalf("sql.Open: %v", err)
	}

	if err := sqlstore.CreateSchema(ctx, db, sqlstore.PostgresDialect{}, sqlstore.DefaultConfig()); err != nil {
		b.Fatalf("CreateSchema: %v", err)
	}

	cleanup := func() {
		db.Close()
		_ = ctr.Terminate(ctx)
	}
	return db, cleanup
}

// ── Task 1: Recovery Performance ─────────────────────────────────────────────

func BenchmarkRecoveryPerformance(b *testing.B) {
	// Spanner Sub-benchmark
	b.Run("Spanner", func(b *testing.B) {
		client, cleanup := setupSpanner(b)
		defer cleanup()

		codec := spannerstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		journal := spannerstore.NewSpannerJournal(client, codec)

		prePopulate(b, journal, recoveryActorCount, recoveryEventCount)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runRecoveryBenchmark(b, journal, nil, recoveryActorCount)
		}
	})

	// Postgres Sub-benchmark
	b.Run("Postgres", func(b *testing.B) {
		db, cleanup := setupPostgres(b)
		defer cleanup()

		codec := sqlstore.NewJSONCodec()
		codec.Register(BenchEvent{})
		journal := sqlstore.NewSQLJournal(db, sqlstore.PostgresDialect{}, codec, sqlstore.DefaultConfig())

		prePopulate(b, journal, recoveryActorCount, recoveryEventCount)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runRecoveryBenchmark(b, journal, nil, recoveryActorCount)
		}
	})
}

// ── Task 2: Snapshot Impact Analysis ─────────────────────────────────────────

func BenchmarkRecoveryWithSnapshots(b *testing.B) {
	db, cleanup := setupPostgres(b) // Use Postgres for snapshot benchmark
	defer cleanup()

	codec := sqlstore.NewJSONCodec()
	codec.Register(BenchEvent{})
	codec.Register(BenchState{})
	journal := sqlstore.NewSQLJournal(db, sqlstore.PostgresDialect{}, codec, sqlstore.DefaultConfig())
	snapshotStore := sqlstore.NewSQLSnapshotStore(db, sqlstore.PostgresDialect{}, codec, sqlstore.DefaultConfig())

	const actorCount = 10
	const eventCount = 100

	b.Run("NoSnapshots", func(b *testing.B) {
		prePopulate(b, journal, actorCount, eventCount)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runRecoveryBenchmark(b, journal, nil, actorCount)
		}
	})

	b.Run("WithSnapshots", func(b *testing.B) {
		prePopulate(b, journal, actorCount, eventCount)
		// Save snapshots for all actors at the end
		ctx := context.Background()
		for i := 0; i < actorCount; i++ {
			pid := fmt.Sprintf("actor-%d", i)
			err := snapshotStore.SaveSnapshot(ctx, persistence.SnapshotMetadata{
				PersistenceID: pid,
				SequenceNr:    uint64(eventCount),
			}, BenchState{Total: eventCount})
			if err != nil {
				b.Fatalf("SaveSnapshot: %v", err)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runRecoveryBenchmark(b, journal, snapshotStore, actorCount)
		}
	})
}

// ── Shared Helpers ───────────────────────────────────────────────────────────

func prePopulate(b *testing.B, journal persistence.Journal, actorCount, eventCount int) {
	ctx := context.Background()
	for i := 0; i < actorCount; i++ {
		pid := fmt.Sprintf("actor-%d", i)
		// Write in batches of 100
		for j := 0; j < eventCount; j += 100 {
			batchSize := 100
			if j+batchSize > eventCount {
				batchSize = eventCount - j
			}
			msgs := make([]persistence.PersistentRepr, batchSize)
			for k := 0; k < batchSize; k++ {
				msgs[k] = persistence.PersistentRepr{
					PersistenceID: pid,
					SequenceNr:    uint64(j + k + 1),
					Payload:       BenchEvent{V: 1},
				}
			}
			if err := journal.AsyncWriteMessages(ctx, msgs); err != nil {
				b.Fatalf("prePopulate: %v", err)
			}
		}
	}
}

func runRecoveryBenchmark(b *testing.B, journal persistence.Journal, snapshotStore persistence.SnapshotStore, actorCount int) {
	system, err := gekka.NewActorSystem("bench")
	if err != nil {
		b.Fatalf("NewActorSystem: %v", err)
	}
	defer func() {
		system.Terminate()
		<-system.WhenTerminated()
	}()

	var wg sync.WaitGroup
	wg.Add(actorCount)

	// Responder behavior to handle Pongs
	replyBehavior := func(ctx typed.TypedContext[BenchPong], msg BenchPong) typed.Behavior[BenchPong] {
		wg.Done()
		return typed.Stopped[BenchPong]()
	}
	replyRef, _ := gekka.Spawn(system, replyBehavior, "responder")

	for i := 0; i < actorCount; i++ {
		pid := fmt.Sprintf("actor-%d", i)
		behavior := benchBehavior(pid, journal, snapshotStore, 0)
		ref, err := gekka.SpawnPersistent(system, behavior, pid)
		if err != nil {
			b.Fatalf("SpawnPersistent: %v", err)
		}
		// Send Ping to verify recovery is complete
		ref.Tell(BenchCommand{ReplyTo: replyRef})
	}

	wg.Wait()
}

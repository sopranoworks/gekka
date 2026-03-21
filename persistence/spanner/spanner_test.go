/*
 * spanner_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

//go:build spanner

package spannerstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/sopranoworks/gekka/persistence"
	spannerstore "github.com/sopranoworks/gekka/persistence/spanner"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

// ── Emulator lifecycle ────────────────────────────────────────────────────────

const (
	testProject  = "test-project"
	testInstance = "test-instance"
	testDatabase = "test-db"
)

// emulatorFixture holds the container and the fully-qualified Spanner database
// name used by all tests in this package.
type emulatorFixture struct {
	endpoint string // host:port of the emulator gRPC endpoint
	dbName   string // projects/.../instances/.../databases/...
}

var (
	fixtureOnce sync.Once
	fixture     *emulatorFixture
	fixtureErr  error
)

// getFixture starts the Spanner emulator container and creates the instance,
// database, and schema.  It is called at most once per test binary run.
func getFixture(t *testing.T) *emulatorFixture {
	t.Helper()
	fixtureOnce.Do(func() {
		ctx := context.Background()

		// Start emulator container.
		ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
			ContainerRequest: tc.ContainerRequest{
				Image:        "gcr.io/cloud-spanner-emulator/emulator:latest",
				ExposedPorts: []string{"9010/tcp"},
				WaitingFor:   wait.ForListeningPort("9010/tcp").WithStartupTimeout(60 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			fixtureErr = fmt.Errorf("start emulator: %w", err)
			return
		}

		host, err := ctr.Host(ctx)
		if err != nil {
			fixtureErr = fmt.Errorf("emulator host: %w", err)
			return
		}
		port, err := ctr.MappedPort(ctx, "9010/tcp")
		if err != nil {
			fixtureErr = fmt.Errorf("emulator port: %w", err)
			return
		}
		endpoint := fmt.Sprintf("%s:%s", host, port.Port())

		opts := []option.ClientOption{
			option.WithEndpoint(endpoint),
			option.WithoutAuthentication(),
		}

		// Create Spanner instance.
		instAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
		if err != nil {
			fixtureErr = fmt.Errorf("instance admin client: %w", err)
			return
		}
		defer instAdmin.Close()

		instOp, err := instAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", testProject),
			InstanceId: testInstance,
			Instance: &instancepb.Instance{
				Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", testProject),
				DisplayName: "Test Instance",
				NodeCount:   1,
			},
		})
		if err != nil {
			fixtureErr = fmt.Errorf("create instance: %w", err)
			return
		}
		if _, err := instOp.Wait(ctx); err != nil {
			fixtureErr = fmt.Errorf("wait for instance: %w", err)
			return
		}

		// Create database with schema.
		dbAdmin, err := database.NewDatabaseAdminClient(ctx, opts...)
		if err != nil {
			fixtureErr = fmt.Errorf("database admin client: %w", err)
			return
		}
		defer dbAdmin.Close()

		dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
			testProject, testInstance, testDatabase)
		dbOp, err := dbAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
			Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
			ExtraStatements: []string{
				spannerstore.JournalDDL,
				spannerstore.SnapshotsDDL,
			},
		})
		if err != nil {
			fixtureErr = fmt.Errorf("create database: %w", err)
			return
		}
		if _, err := dbOp.Wait(ctx); err != nil {
			fixtureErr = fmt.Errorf("wait for database: %w", err)
			return
		}

		fixture = &emulatorFixture{endpoint: endpoint, dbName: dbName}
	})

	if fixtureErr != nil {
		t.Skipf("spanner emulator unavailable: %v", fixtureErr)
	}
	return fixture
}

// newClient returns a *spanner.Client wired to the emulator fixture.
func newClient(t *testing.T, f *emulatorFixture) *spanner.Client {
	t.Helper()
	opts := []option.ClientOption{
		option.WithEndpoint(f.endpoint),
		option.WithoutAuthentication(),
	}
	client, err := spanner.NewClient(context.Background(), f.dbName, opts...)
	if err != nil {
		t.Fatalf("spanner.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// ── Journal tests ─────────────────────────────────────────────────────────────

type OrderPlaced struct{ Item string }
type Evt struct{ V int }

func TestSpanner_Journal_WriteAndReplay(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := spannerstore.NewSpannerJournal(client, codec)
	ctx := context.Background()
	const pid = "spanner-order-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "banana"}},
	}
	if err := j.AsyncWriteMessages(ctx, events); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 2 {
		t.Errorf("HighestSequenceNr = %d, want 2", highest)
	}

	var replayed []persistence.PersistentRepr
	if err := j.ReplayMessages(ctx, pid, 1, 2, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %d events, want 2", len(replayed))
	}
	if ev, ok := replayed[0].Payload.(OrderPlaced); !ok || ev.Item != "apple" {
		t.Errorf("replayed[0] = %v, want OrderPlaced{apple}", replayed[0].Payload)
	}
	if ev, ok := replayed[1].Payload.(OrderPlaced); !ok || ev.Item != "banana" {
		t.Errorf("replayed[1] = %v, want OrderPlaced{banana}", replayed[1].Payload)
	}
}

func TestSpanner_Journal_DeleteMessagesTo(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	j := spannerstore.NewSpannerJournal(client, codec)
	ctx := context.Background()
	const pid = "spanner-del-actor"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: Evt{1}},
		{PersistenceID: pid, SequenceNr: 2, Payload: Evt{2}},
		{PersistenceID: pid, SequenceNr: 3, Payload: Evt{3}},
	}
	if err := j.AsyncWriteMessages(ctx, events); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}
	if err := j.AsyncDeleteMessagesTo(ctx, pid, 2); err != nil {
		t.Fatalf("AsyncDeleteMessagesTo: %v", err)
	}

	var replayed []persistence.PersistentRepr
	if err := j.ReplayMessages(ctx, pid, 1, 9999, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}); err != nil {
		t.Fatalf("ReplayMessages after delete: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("replayed %d events, want 1", len(replayed))
	}
	if ev, ok := replayed[0].Payload.(Evt); !ok || ev.V != 3 {
		t.Errorf("replayed[0] = %v, want Evt{3}", replayed[0].Payload)
	}
}

func TestSpanner_Journal_IdempotentWrites(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	j := spannerstore.NewSpannerJournal(client, codec)
	ctx := context.Background()
	const pid = "spanner-idem-actor"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: Evt{42}},
	}
	if err := j.AsyncWriteMessages(ctx, events); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := j.AsyncWriteMessages(ctx, events); err != nil {
		t.Fatalf("second write (idempotent): %v", err)
	}

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 1 {
		t.Errorf("HighestSequenceNr = %d, want 1", highest)
	}
}

// TestSpanner_Journal_ConcurrentAppendConflict verifies that two goroutines
// racing to write disjoint sequence numbers for the same actor both succeed
// (Spanner handles concurrent mutations at different PKs independently).
func TestSpanner_Journal_ConcurrentAppendConflict(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	j := spannerstore.NewSpannerJournal(client, codec)
	ctx := context.Background()
	const pid = "spanner-concurrent"

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	for _, seqNr := range []uint64{10, 11} {
		seqNr := seqNr
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := j.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
				{PersistenceID: pid, SequenceNr: seqNr, Payload: Evt{V: int(seqNr)}},
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent write error: %v", err)
	}

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 11 {
		t.Errorf("HighestSequenceNr = %d, want 11", highest)
	}
}

// ── SnapshotStore tests ───────────────────────────────────────────────────────

type State struct{ Counter int }

func TestSpanner_SnapshotStore_SaveAndLoad(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(State{})

	ss := spannerstore.NewSpannerSnapshotStore(client, codec)
	ctx := context.Background()
	const pid = "spanner-snap-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	if err := ss.SaveSnapshot(ctx, meta, State{Counter: 99}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("LoadSnapshot returned nil, want snapshot")
	}
	if snap.Metadata.SequenceNr != 5 {
		t.Errorf("SequenceNr = %d, want 5", snap.Metadata.SequenceNr)
	}
	st, ok := snap.Snapshot.(State)
	if !ok {
		t.Fatalf("Snapshot type = %T, want State", snap.Snapshot)
	}
	if st.Counter != 99 {
		t.Errorf("Counter = %d, want 99", st.Counter)
	}
}

func TestSpanner_SnapshotStore_Upsert(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(State{})

	ss := spannerstore.NewSpannerSnapshotStore(client, codec)
	ctx := context.Background()
	const pid = "spanner-upsert-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 3, Timestamp: time.Now().UnixNano()}
	if err := ss.SaveSnapshot(ctx, meta, State{Counter: 10}); err != nil {
		t.Fatalf("SaveSnapshot (first): %v", err)
	}
	if err := ss.SaveSnapshot(ctx, meta, State{Counter: 20}); err != nil {
		t.Fatalf("SaveSnapshot (upsert): %v", err)
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("LoadSnapshot returned nil")
	}
	st, _ := snap.Snapshot.(State)
	if st.Counter != 20 {
		t.Errorf("Counter = %d, want 20 (upserted value)", st.Counter)
	}
}

func TestSpanner_SnapshotStore_NoSnapshot_ReturnsNil(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	ss := spannerstore.NewSpannerSnapshotStore(client, codec)

	snap, err := ss.LoadSnapshot(context.Background(), "spanner-nonexistent", persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap != nil {
		t.Errorf("LoadSnapshot = %v, want nil for non-existent actor", snap)
	}
}

func TestSpanner_SnapshotStore_DeleteSnapshot(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(State{})

	ss := spannerstore.NewSpannerSnapshotStore(client, codec)
	ctx := context.Background()
	const pid = "spanner-delsnap-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 7, Timestamp: time.Now().UnixNano()}
	if err := ss.SaveSnapshot(ctx, meta, State{Counter: 7}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := ss.DeleteSnapshot(ctx, meta); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot after delete: %v", err)
	}
	if snap != nil {
		t.Errorf("LoadSnapshot after delete = %v, want nil", snap)
	}
}

func TestSpanner_SnapshotStore_DeleteSnapshots_Range(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(State{})

	ss := spannerstore.NewSpannerSnapshotStore(client, codec)
	ctx := context.Background()
	const pid = "spanner-delrange-actor"

	now := time.Now().UnixNano()
	for i := 1; i <= 5; i++ {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: uint64(i), Timestamp: now + int64(i)}
		if err := ss.SaveSnapshot(ctx, meta, State{Counter: i}); err != nil {
			t.Fatalf("SaveSnapshot %d: %v", i, err)
		}
	}

	// Delete snapshots 1-3, keep 4-5.
	criteria := persistence.SnapshotSelectionCriteria{
		MinSequenceNr: 1, MaxSequenceNr: 3,
		MinTimestamp: now, MaxTimestamp: now + 3,
	}
	if err := ss.DeleteSnapshots(ctx, pid, criteria); err != nil {
		t.Fatalf("DeleteSnapshots: %v", err)
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot after range delete: %v", err)
	}
	if snap == nil {
		t.Fatal("LoadSnapshot returned nil, want snapshot 5")
	}
	if snap.Metadata.SequenceNr != 5 {
		t.Errorf("SequenceNr = %d, want 5", snap.Metadata.SequenceNr)
	}
}

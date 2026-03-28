/*
 * postgres_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

//go:build postgres

package sqltest_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sopranoworks/gekka/persistence"
	sqlstore "github.com/sopranoworks/gekka/persistence/sql"
)

// dsn returns the Postgres DSN from POSTGRES_DSN env variable, or a sensible
// local default.
func dsn() string {
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/gekka_test?sslmode=disable"
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn())
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("postgres unavailable (%v); skipping", err)
	}
	return db
}

func setupSchema(t *testing.T, db *sql.DB, cfg sqlstore.Config) {
	t.Helper()
	ctx := context.Background()
	jt := cfg.JournalTable
	if jt == "" {
		jt = "journal"
	}
	st := cfg.SnapshotTable
	if st == "" {
		st = "snapshots"
	}
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+jt)
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+st)
	if err := sqlstore.CreateSchema(ctx, db, sqlstore.PostgresDialect{}, cfg); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
}

// ── Journal tests ─────────────────────────────────────────────────────────────

func TestPostgres_Journal_WriteAndReplay(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type OrderPlaced struct{ Item string }

	codec := sqlstore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	cfg := sqlstore.DefaultConfig()
	setupSchema(t, db, cfg)

	sqlstore.RegisterPostgresBackend("pg-journal-test", codec, cfg)

	j, err := persistence.NewJournalFromDB("pg-journal-test", db)
	if err != nil {
		t.Fatalf("NewJournalFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "order-1"

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
	err = j.ReplayMessages(ctx, pid, 1, 2, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	})
	if err != nil {
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

func TestPostgres_Journal_DeleteMessagesTo(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type Evt struct{ V int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(Evt{})

	cfg := sqlstore.Config{JournalTable: "journal_del_test", SnapshotTable: "snapshots_del_test"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-del-test", codec, cfg)

	j, err := persistence.NewJournalFromDB("pg-del-test", db)
	if err != nil {
		t.Fatalf("NewJournalFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "del-actor"

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

	// Replay from 1 — only seq 3 should come back (deleted rows are excluded).
	var replayed []persistence.PersistentRepr
	if err := j.ReplayMessages(ctx, pid, 1, 9999, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}); err != nil {
		t.Fatalf("ReplayMessages after delete: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("replayed %d events after delete, want 1", len(replayed))
	}
	if ev, ok := replayed[0].Payload.(Evt); !ok || ev.V != 3 {
		t.Errorf("replayed[0] = %v, want Evt{3}", replayed[0].Payload)
	}
}

func TestPostgres_Journal_IdempotentWrites(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type Evt struct{ V int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(Evt{})

	cfg := sqlstore.Config{JournalTable: "journal_idem", SnapshotTable: "snapshots_idem"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-idem", codec, cfg)

	j, err := persistence.NewJournalFromDB("pg-idem", db)
	if err != nil {
		t.Fatalf("NewJournalFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "idem-actor"

	events := []persistence.PersistentRepr{{PersistenceID: pid, SequenceNr: 1, Payload: Evt{42}}}

	// Write twice — second write is silently ignored (ON CONFLICT DO NOTHING).
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
		t.Errorf("HighestSequenceNr = %d, want 1 (no duplicates)", highest)
	}
}

// ── SnapshotStore tests ───────────────────────────────────────────────────────

func TestPostgres_SnapshotStore_SaveAndLoad(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type State struct{ Counter int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(State{})

	cfg := sqlstore.DefaultConfig()
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-snap-test", codec, cfg)

	ss, err := persistence.NewSnapshotStoreFromDB("pg-snap-test", db)
	if err != nil {
		t.Fatalf("NewSnapshotStoreFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "snap-actor"

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

func TestPostgres_SnapshotStore_Upsert(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type State struct{ Counter int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(State{})

	cfg := sqlstore.Config{JournalTable: "journal_upsert", SnapshotTable: "snapshots_upsert"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-upsert", codec, cfg)

	ss, err := persistence.NewSnapshotStoreFromDB("pg-upsert", db)
	if err != nil {
		t.Fatalf("NewSnapshotStoreFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "upsert-actor"

	// Two saves at the same sequence number — second should overwrite.
	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 3}
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

func TestPostgres_SnapshotStore_NoSnapshot_ReturnsNil(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	codec := sqlstore.NewJSONCodec()
	cfg := sqlstore.Config{JournalTable: "journal_nosnap", SnapshotTable: "snapshots_nosnap"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-nosnap", codec, cfg)

	ss, err := persistence.NewSnapshotStoreFromDB("pg-nosnap", db)
	if err != nil {
		t.Fatalf("NewSnapshotStoreFromDB: %v", err)
	}

	snap, err := ss.LoadSnapshot(context.Background(), "nonexistent-actor", persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap != nil {
		t.Errorf("LoadSnapshot = %v, want nil for non-existent actor", snap)
	}
}

func TestPostgres_SnapshotStore_DeleteSnapshot(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type State struct{ V int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(State{})

	cfg := sqlstore.Config{JournalTable: "journal_delsnap", SnapshotTable: "snapshots_delsnap"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-delsnap", codec, cfg)

	ss, err := persistence.NewSnapshotStoreFromDB("pg-delsnap", db)
	if err != nil {
		t.Fatalf("NewSnapshotStoreFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "delsnap-actor"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 7}
	if err := ss.SaveSnapshot(ctx, meta, State{V: 7}); err != nil {
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

func TestPostgres_SnapshotStore_DeleteSnapshots(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	type State struct{ V int }
	codec := sqlstore.NewJSONCodec()
	codec.Register(State{})

	cfg := sqlstore.Config{JournalTable: "journal_delrange", SnapshotTable: "snapshots_delrange"}
	setupSchema(t, db, cfg)
	sqlstore.RegisterPostgresBackend("pg-delrange", codec, cfg)

	ss, err := persistence.NewSnapshotStoreFromDB("pg-delrange", db)
	if err != nil {
		t.Fatalf("NewSnapshotStoreFromDB: %v", err)
	}

	ctx := context.Background()
	const pid = "delrange-actor"

	// Save 5 snapshots at seqNr 1..5.
	for i := 1; i <= 5; i++ {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: uint64(i)}
		if err := ss.SaveSnapshot(ctx, meta, State{V: i}); err != nil {
			t.Fatalf("SaveSnapshot(%d): %v", i, err)
		}
	}

	// Delete seqNr 1..3 (keep 4 and 5).
	criteria := persistence.SnapshotSelectionCriteria{MaxSequenceNr: 3, MaxTimestamp: 1<<62 - 1}
	if err := ss.DeleteSnapshots(ctx, pid, criteria); err != nil {
		t.Fatalf("DeleteSnapshots: %v", err)
	}

	// seqNr <= 3 must be gone.
	snap, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{MaxSequenceNr: 3, MaxTimestamp: 1<<62 - 1})
	if err != nil {
		t.Fatalf("LoadSnapshot after delete range: %v", err)
	}
	if snap != nil {
		t.Errorf("expected no snapshot with seqNr <= 3, got seqNr=%d", snap.Metadata.SequenceNr)
	}

	// Latest snapshot must be seqNr 5.
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot latest: %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot at seqNr=5, got nil")
	}
	if snap.Metadata.SequenceNr != 5 {
		t.Errorf("latest seqNr = %d, want 5", snap.Metadata.SequenceNr)
	}
}

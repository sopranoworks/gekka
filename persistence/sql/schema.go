/*
 * schema.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sopranoworks/gekka/persistence"
)

// CreateSchema creates the journal and snapshot tables (and indexes) in db
// using the given dialect's DDL statements.  It is idempotent: IF NOT EXISTS
// guards prevent errors on subsequent calls.
//
// Call this once at application startup, before spawning any persistent actors.
//
//	db, _ := sql.Open("pgx", dsn)
//	if err := sqlstore.CreateSchema(ctx, db, sqlstore.PostgresDialect{}, sqlstore.DefaultConfig()); err != nil {
//	    log.Fatal(err)
//	}
//
// If you prefer to manage schema migrations yourself, skip this helper and
// apply the DDL from Dialect.CreateJournalTableSQL / CreateSnapshotTableSQL
// using your own tooling (Flyway, Liquibase, goose, …).
func CreateSchema(ctx context.Context, db *sql.DB, dialect Dialect, config Config) error {
	if _, err := db.ExecContext(ctx, dialect.CreateJournalTableSQL(config.journalTable())); err != nil {
		return fmt.Errorf("sqlstore: create journal table %q: %w", config.journalTable(), err)
	}
	if _, err := db.ExecContext(ctx, dialect.CreateSnapshotTableSQL(config.snapshotTable())); err != nil {
		return fmt.Errorf("sqlstore: create snapshot table %q: %w", config.snapshotTable(), err)
	}
	return nil
}

// RegisterPostgresBackend registers PostgreSQL-backed Journal and SnapshotStore
// factories under name in the persistence registry.  Import this package (or
// call this function) once at startup to enable name-based construction via
// persistence.NewJournalFromDB and persistence.NewSnapshotStoreFromDB.
//
// codec is shared by both factories; it should have all event and state types
// registered before the first DB connection is provisioned.
//
//	codec := sqlstore.NewJSONCodec()
//	codec.Register(MyEvent{})
//	codec.Register(MyState{})
//	sqlstore.RegisterPostgresBackend("postgres", codec, sqlstore.DefaultConfig())
//
//	// Later:
//	journal, _ := persistence.NewJournalFromDB("postgres", db)
func RegisterPostgresBackend(name string, codec PayloadCodec, config Config) {
	persistence.RegisterJournal(name, func(db *sql.DB) persistence.Journal {
		return NewSQLJournal(db, PostgresDialect{}, codec, config)
	})
	persistence.RegisterSnapshotStore(name, func(db *sql.DB) persistence.SnapshotStore {
		return NewSQLSnapshotStore(db, PostgresDialect{}, codec, config)
	})
}

// RegisterMySQLBackend registers MySQL-backed Journal and SnapshotStore
// factories under name in the persistence registry.
func RegisterMySQLBackend(name string, codec PayloadCodec, config Config) {
	persistence.RegisterJournal(name, func(db *sql.DB) persistence.Journal {
		return NewSQLJournal(db, MySQLDialect{}, codec, config)
	})
	persistence.RegisterSnapshotStore(name, func(db *sql.DB) persistence.SnapshotStore {
		return NewSQLSnapshotStore(db, MySQLDialect{}, codec, config)
	})
}

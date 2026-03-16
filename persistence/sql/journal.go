/*
 * journal.go
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
	"math"
	"time"

	"github.com/sopranoworks/gekka/persistence"
)

// SQLJournal implements persistence.Journal on top of any database/sql-compatible
// database.  It stores events as JSON-encoded BLOB/BYTEA rows and uses a
// Dialect to generate the correct SQL for the target database.
//
// Create one with NewSQLJournal and call CreateSchema (or run the DDL manually)
// before the first use.
type SQLJournal struct {
	db      *sql.DB
	dialect Dialect
	codec   PayloadCodec
	table   string
}

// NewSQLJournal creates a SQLJournal.
//
//   - db      — a pre-opened *sql.DB (any driver that speaks database/sql)
//   - dialect — SQL syntax variant (e.g. PostgresDialect{} or MySQLDialect{})
//   - codec   — payload serializer; must have all event types registered
//   - config  — table-name configuration (use DefaultConfig() for defaults)
//
// The journal does not call CreateSchema; run it once at application startup:
//
//	if err := sqlstore.CreateSchema(ctx, db, sqlstore.PostgresDialect{}, sqlstore.DefaultConfig()); err != nil {
//	    log.Fatal(err)
//	}
func NewSQLJournal(db *sql.DB, dialect Dialect, codec PayloadCodec, config Config) *SQLJournal {
	return &SQLJournal{
		db:      db,
		dialect: dialect,
		codec:   codec,
		table:   config.journalTable(),
	}
}

// ── persistence.Journal implementation ───────────────────────────────────────

// AsyncWriteMessages writes a batch of messages atomically within a single
// transaction.  The write is idempotent: duplicate (persistenceId, sequenceNr)
// pairs are silently ignored by the ON CONFLICT / INSERT IGNORE clause.
func (j *SQLJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqljournal: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	stmt, err := tx.PrepareContext(ctx, j.dialect.JournalInsertSQL(j.table))
	if err != nil {
		return fmt.Errorf("sqljournal: prepare insert: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UnixNano()
	for _, msg := range messages {
		manifest, data, err := j.codec.Encode(msg.Payload)
		if err != nil {
			return fmt.Errorf("sqljournal: encode payload for %s/%d: %w",
				msg.PersistenceID, msg.SequenceNr, err)
		}
		if _, err = stmt.ExecContext(ctx,
			msg.PersistenceID,
			int64(msg.SequenceNr),
			data,
			manifest,
			msg.SenderPath,
			msg.Deleted,
			now,
		); err != nil {
			return fmt.Errorf("sqljournal: insert %s/%d: %w",
				msg.PersistenceID, msg.SequenceNr, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqljournal: commit: %w", err)
	}
	return nil
}

// ReadHighestSequenceNr returns the highest stored sequence number for
// persistenceId that is ≥ fromSequenceNr.  Returns 0 if no events exist.
func (j *SQLJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	row := j.db.QueryRowContext(ctx,
		j.dialect.JournalHighestSeqNrSQL(j.table),
		persistenceId,
		int64(fromSequenceNr),
	)
	var highest int64
	if err := row.Scan(&highest); err != nil {
		return 0, fmt.Errorf("sqljournal: read highest seq nr for %q: %w", persistenceId, err)
	}
	if highest < 0 {
		return 0, nil
	}
	return uint64(highest), nil
}

// ReplayMessages queries events for persistenceId in [fromSequenceNr, toSequenceNr]
// and calls callback for each.  At most max events are replayed; pass 0 for
// unlimited (internally capped at math.MaxInt64).
func (j *SQLJournal) ReplayMessages(
	ctx context.Context,
	persistenceId string,
	fromSequenceNr, toSequenceNr uint64,
	max uint64,
	callback func(persistence.PersistentRepr),
) error {
	limit := int64(max)
	if limit <= 0 {
		limit = math.MaxInt64
	}

	rows, err := j.db.QueryContext(ctx,
		j.dialect.JournalReplaySQL(j.table),
		persistenceId,
		int64(fromSequenceNr),
		int64(toSequenceNr),
		limit,
	)
	if err != nil {
		return fmt.Errorf("sqljournal: replay query for %q: %w", persistenceId, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			pid        string
			seqNr      int64
			rawPayload []byte
			manifest   string
			senderPath string
		)
		if err := rows.Scan(&pid, &seqNr, &rawPayload, &manifest, &senderPath); err != nil {
			return fmt.Errorf("sqljournal: scan row for %q: %w", persistenceId, err)
		}

		payload, err := j.codec.Decode(manifest, rawPayload)
		if err != nil {
			return fmt.Errorf("sqljournal: decode payload %q/%d (manifest=%q): %w",
				persistenceId, seqNr, manifest, err)
		}

		callback(persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    uint64(seqNr),
			Payload:       payload,
			SenderPath:    senderPath,
		})
	}
	return rows.Err()
}

// AsyncDeleteMessagesTo deletes all events for persistenceId with sequence
// numbers ≤ toSequenceNr.
func (j *SQLJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	_, err := j.db.ExecContext(ctx,
		j.dialect.JournalDeleteSQL(j.table),
		persistenceId,
		int64(toSequenceNr),
	)
	if err != nil {
		return fmt.Errorf("sqljournal: delete for %q up to %d: %w", persistenceId, toSequenceNr, err)
	}
	return nil
}

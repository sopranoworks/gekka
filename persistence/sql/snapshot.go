/*
 * snapshot.go
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

// SQLSnapshotStore implements persistence.SnapshotStore on top of any
// database/sql-compatible database.
//
// Snapshots are serialised to bytes using a PayloadCodec and stored in the
// table specified by Config.SnapshotTable.  A Dialect abstracts the
// SQL syntax differences between database vendors.
//
// Create one with NewSQLSnapshotStore and call CreateSchema (or run the DDL
// manually) before first use.
type SQLSnapshotStore struct {
	db      *sql.DB
	dialect Dialect
	codec   PayloadCodec
	table   string
}

// NewSQLSnapshotStore creates a SQLSnapshotStore.
//
//   - db      — a pre-opened *sql.DB (any driver that speaks database/sql)
//   - dialect — SQL syntax variant (e.g. PostgresDialect{} or MySQLDialect{})
//   - codec   — payload serializer; must have all snapshot state types registered
//   - config  — table-name configuration (use DefaultConfig() for defaults)
func NewSQLSnapshotStore(db *sql.DB, dialect Dialect, codec PayloadCodec, config Config) *SQLSnapshotStore {
	return &SQLSnapshotStore{
		db:      db,
		dialect: dialect,
		codec:   codec,
		table:   config.snapshotTable(),
	}
}

// ── persistence.SnapshotStore implementation ─────────────────────────────────

// SaveSnapshot persists a snapshot for the given metadata.  If a snapshot
// with the same (persistenceId, sequenceNr) already exists it is overwritten
// (upsert semantics).
func (s *SQLSnapshotStore) SaveSnapshot(
	ctx context.Context,
	metadata persistence.SnapshotMetadata,
	snapshot any,
) error {
	manifest, data, err := s.codec.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("sqlsnapshot: encode snapshot for %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}

	ts := metadata.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	_, err = s.db.ExecContext(ctx,
		s.dialect.SnapshotUpsertSQL(s.table),
		metadata.PersistenceID,
		int64(metadata.SequenceNr),
		ts,
		data,
		manifest,
	)
	if err != nil {
		return fmt.Errorf("sqlsnapshot: upsert for %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// LoadSnapshot returns the most recent snapshot for persistenceId that
// satisfies all fields of criteria.  Returns nil, nil when no matching
// snapshot exists.
func (s *SQLSnapshotStore) LoadSnapshot(
	ctx context.Context,
	persistenceId string,
	criteria persistence.SnapshotSelectionCriteria,
) (*persistence.SelectedSnapshot, error) {
	maxSeqNr := int64(math.MaxInt64)
	if criteria.MaxSequenceNr <= math.MaxInt64 {
		maxSeqNr = int64(criteria.MaxSequenceNr)
	}

	row := s.db.QueryRowContext(ctx,
		s.dialect.SnapshotSelectSQL(s.table),
		persistenceId,
		int64(criteria.MinSequenceNr),
		maxSeqNr,
		criteria.MinTimestamp,
		criteria.MaxTimestamp,
	)

	var (
		seqNr    int64
		ts       int64
		rawSnap  []byte
		manifest string
	)
	err := row.Scan(&seqNr, &ts, &rawSnap, &manifest)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlsnapshot: load for %q: %w", persistenceId, err)
	}

	snap, err := s.codec.Decode(manifest, rawSnap)
	if err != nil {
		return nil, fmt.Errorf("sqlsnapshot: decode snapshot %q/%d (manifest=%q): %w",
			persistenceId, seqNr, manifest, err)
	}
	return &persistence.SelectedSnapshot{
		Metadata: persistence.SnapshotMetadata{
			PersistenceID: persistenceId,
			SequenceNr:    uint64(seqNr),
			Timestamp:     ts,
		},
		Snapshot: snap,
	}, nil
}

// DeleteSnapshot removes the snapshot identified by metadata.SequenceNr for
// metadata.PersistenceID.
func (s *SQLSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	_, err := s.db.ExecContext(ctx,
		s.dialect.SnapshotDeleteSQL(s.table),
		metadata.PersistenceID,
		int64(metadata.SequenceNr),
	)
	if err != nil {
		return fmt.Errorf("sqlsnapshot: delete %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// DeleteSnapshots removes all snapshots for persistenceId that match the
// given selection criteria.
func (s *SQLSnapshotStore) DeleteSnapshots(
	ctx context.Context,
	persistenceId string,
	criteria persistence.SnapshotSelectionCriteria,
) error {
	maxSeqNrDel := int64(math.MaxInt64)
	if criteria.MaxSequenceNr <= math.MaxInt64 {
		maxSeqNrDel = int64(criteria.MaxSequenceNr)
	}

	_, err := s.db.ExecContext(ctx,
		s.dialect.SnapshotDeleteRangeSQL(s.table),
		persistenceId,
		int64(criteria.MinSequenceNr),
		maxSeqNrDel,
		criteria.MinTimestamp,
		criteria.MaxTimestamp,
	)
	if err != nil {
		return fmt.Errorf("sqlsnapshot: delete range for %q: %w", persistenceId, err)
	}
	return nil
}

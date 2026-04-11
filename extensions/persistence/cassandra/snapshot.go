/*
 * snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraSnapshotStore implements persistence.SnapshotStore backed by
// Apache Cassandra using the Pekko-compatible snapshots schema.
type CassandraSnapshotStore struct {
	session  *gocql.Session
	keyspace string
	table    string
	codec    PayloadCodec
}

// NewCassandraSnapshotStore constructs a CassandraSnapshotStore from a live
// session and config.
func NewCassandraSnapshotStore(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraSnapshotStore {
	return &CassandraSnapshotStore{
		session:  session,
		keyspace: cfg.SnapshotKeyspace,
		table:    cfg.SnapshotTable,
		codec:    codec,
	}
}

// SaveSnapshot encodes snapshot and persists it to Cassandra.
// If metadata.Timestamp is zero, the current time (UnixNano) is used.
func (s *CassandraSnapshotStore) SaveSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata, snapshot any) error {
	manifest, data, err := s.codec.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("cassandrasnapshotstore: encode %q seq %d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}

	ts := metadata.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	q := fmt.Sprintf(
		`INSERT INTO %s.%s (persistence_id, sequence_nr, timestamp, ser_id, ser_manifest, snapshot_data) VALUES (?, ?, ?, ?, ?, ?)`,
		s.keyspace, s.table,
	)
	if err := s.session.Query(q,
		metadata.PersistenceID,
		int64(metadata.SequenceNr), //nolint:gosec
		ts,
		0,
		manifest,
		data,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrasnapshotstore: save %q seq %d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// LoadSnapshot finds the newest snapshot for pid satisfying criteria.
// Cassandra returns rows in descending sequence_nr order; the first row whose
// timestamp falls within [minTs, maxTs] is returned.
func (s *CassandraSnapshotStore) LoadSnapshot(ctx context.Context, pid string, criteria persistence.SnapshotSelectionCriteria) (*persistence.SelectedSnapshot, error) {
	maxSeq := criteria.MaxSequenceNr
	if maxSeq == 0 {
		maxSeq = ^uint64(0)
	}
	minSeq := criteria.MinSequenceNr

	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	q := fmt.Sprintf(
		`SELECT sequence_nr, timestamp, ser_manifest, snapshot_data
		 FROM %s.%s
		 WHERE persistence_id = ? AND sequence_nr <= ? AND sequence_nr >= ?
		 ORDER BY sequence_nr DESC`,
		s.keyspace, s.table,
	)
	iter := s.session.Query(q,
		pid,
		int64(maxSeq), //nolint:gosec
		int64(minSeq), //nolint:gosec
	).WithContext(ctx).Iter()

	var (
		seqNr    int64
		ts       int64
		manifest string
		data     []byte
	)
	for iter.Scan(&seqNr, &ts, &manifest, &data) {
		if ts < minTs || ts > maxTs {
			continue
		}
		payload, decErr := s.codec.Decode(manifest, data)
		if decErr != nil {
			_ = iter.Close()
			return nil, fmt.Errorf("cassandrasnapshotstore: decode %q seq %d: %w", pid, seqNr, decErr)
		}
		_ = iter.Close()
		return &persistence.SelectedSnapshot{
			Metadata: persistence.SnapshotMetadata{
				PersistenceID: pid,
				SequenceNr:    uint64(seqNr), //nolint:gosec
				Timestamp:     ts,
			},
			Snapshot: payload,
		}, nil
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("cassandrasnapshotstore: load %q: %w", pid, err)
	}
	return nil, nil
}

// DeleteSnapshot deletes the snapshot identified by metadata.
func (s *CassandraSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	q := fmt.Sprintf(
		`DELETE FROM %s.%s WHERE persistence_id = ? AND sequence_nr = ?`,
		s.keyspace, s.table,
	)
	if err := s.session.Query(q,
		metadata.PersistenceID,
		int64(metadata.SequenceNr), //nolint:gosec
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrasnapshotstore: delete %q seq %d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// DeleteSnapshots deletes all snapshots for pid that satisfy criteria using a
// read-then-delete pattern (required by Cassandra's PRIMARY KEY structure).
func (s *CassandraSnapshotStore) DeleteSnapshots(ctx context.Context, pid string, criteria persistence.SnapshotSelectionCriteria) error {
	maxSeq := criteria.MaxSequenceNr
	if maxSeq == 0 {
		maxSeq = ^uint64(0)
	}
	minSeq := criteria.MinSequenceNr

	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	selectQ := fmt.Sprintf(
		`SELECT sequence_nr, timestamp FROM %s.%s
		 WHERE persistence_id = ? AND sequence_nr <= ? AND sequence_nr >= ?`,
		s.keyspace, s.table,
	)
	iter := s.session.Query(selectQ,
		pid,
		int64(maxSeq), //nolint:gosec
		int64(minSeq), //nolint:gosec
	).WithContext(ctx).Iter()

	deleteQ := fmt.Sprintf(
		`DELETE FROM %s.%s WHERE persistence_id = ? AND sequence_nr = ?`,
		s.keyspace, s.table,
	)

	var (
		seqNr int64
		ts    int64
	)
	for iter.Scan(&seqNr, &ts) {
		if ts < minTs || ts > maxTs {
			continue
		}
		if err := s.session.Query(deleteQ, pid, seqNr).WithContext(ctx).Exec(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("cassandrasnapshotstore: delete %q seq %d: %w", pid, seqNr, err)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("cassandrasnapshotstore: delete scan %q: %w", pid, err)
	}
	return nil
}

// Compile-time check that CassandraSnapshotStore satisfies persistence.SnapshotStore.
var _ persistence.SnapshotStore = (*CassandraSnapshotStore)(nil)

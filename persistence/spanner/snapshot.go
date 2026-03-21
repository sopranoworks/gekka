/*
 * snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"context"
	"fmt"
	"math"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sopranoworks/gekka/persistence"
	"google.golang.org/api/iterator"
)

// SpannerSnapshotStore implements persistence.SnapshotStore on top of Cloud Spanner.
//
// Snapshots are stored in the `snapshots` table (see SnapshotsDDL).  Saves use
// InsertOrUpdate mutations (upsert semantics); loads use read-only transactions.
type SpannerSnapshotStore struct {
	client *spanner.Client
	codec  PayloadCodec
}

// NewSpannerSnapshotStore creates a SpannerSnapshotStore.
//
//   - client — an open *spanner.Client pointing at the target database
//   - codec  — payload serialiser; all snapshot state types must be registered
func NewSpannerSnapshotStore(client *spanner.Client, codec PayloadCodec) *SpannerSnapshotStore {
	return &SpannerSnapshotStore{client: client, codec: codec}
}

// SaveSnapshot persists a snapshot using InsertOrUpdate (upsert) semantics.
// A second save for the same (persistenceId, sequenceNr) overwrites the first.
func (s *SpannerSnapshotStore) SaveSnapshot(
	ctx context.Context,
	metadata persistence.SnapshotMetadata,
	snapshot any,
) error {
	manifest, data, err := s.codec.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("spannersnapshot: encode %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}
	ts := metadata.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}
	mut := spanner.InsertOrUpdate("snapshots",
		[]string{"persistence_id", "sequence_nr", "snapshot_ts", "snapshot", "manifest"},
		[]any{
			metadata.PersistenceID,
			int64(metadata.SequenceNr),
			ts,
			data,
			manifest,
		},
	)
	_, err = s.client.Apply(ctx, []*spanner.Mutation{mut})
	if err != nil {
		return fmt.Errorf("spannersnapshot: apply upsert for %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// LoadSnapshot returns the most recent snapshot satisfying all criteria fields,
// or nil when no matching snapshot exists.
func (s *SpannerSnapshotStore) LoadSnapshot(
	ctx context.Context,
	persistenceId string,
	criteria persistence.SnapshotSelectionCriteria,
) (*persistence.SelectedSnapshot, error) {
	maxSeqNr := int64(math.MaxInt64)
	if criteria.MaxSequenceNr <= math.MaxInt64 {
		maxSeqNr = int64(criteria.MaxSequenceNr)
	}
	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}

	stmt := spanner.Statement{
		SQL: `SELECT sequence_nr, snapshot_ts, snapshot, manifest
		      FROM snapshots
		      WHERE persistence_id  = @pid
		        AND sequence_nr    >= @minSeq
		        AND sequence_nr    <= @maxSeq
		        AND snapshot_ts    >= @minTs
		        AND snapshot_ts    <= @maxTs
		      ORDER BY sequence_nr DESC
		      LIMIT 1`,
		Params: map[string]any{
			"pid":    persistenceId,
			"minSeq": int64(criteria.MinSequenceNr),
			"maxSeq": maxSeqNr,
			"minTs":  criteria.MinTimestamp,
			"maxTs":  maxTs,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("spannersnapshot: load for %q: %w", persistenceId, err)
	}

	var (
		seqNr    int64
		ts       int64
		rawSnap  []byte
		manifest string
	)
	if err := row.Columns(&seqNr, &ts, &rawSnap, &manifest); err != nil {
		return nil, fmt.Errorf("spannersnapshot: scan for %q: %w", persistenceId, err)
	}

	snap, err := s.codec.Decode(manifest, rawSnap)
	if err != nil {
		return nil, fmt.Errorf("spannersnapshot: decode %q/%d (manifest=%q): %w",
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

// DeleteSnapshot removes the snapshot identified by (metadata.PersistenceID,
// metadata.SequenceNr).
func (s *SpannerSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	key := spanner.Key{metadata.PersistenceID, int64(metadata.SequenceNr)}
	_, err := s.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("snapshots", key),
	})
	if err != nil {
		return fmt.Errorf("spannersnapshot: delete %q/%d: %w",
			metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// DeleteSnapshots removes all snapshots matching criteria using partitioned DML.
func (s *SpannerSnapshotStore) DeleteSnapshots(
	ctx context.Context,
	persistenceId string,
	criteria persistence.SnapshotSelectionCriteria,
) error {
	maxSeqNr := int64(math.MaxInt64)
	if criteria.MaxSequenceNr <= math.MaxInt64 {
		maxSeqNr = int64(criteria.MaxSequenceNr)
	}
	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}

	stmt := spanner.Statement{
		SQL: `DELETE FROM snapshots
		      WHERE persistence_id = @pid
		        AND sequence_nr   >= @minSeq
		        AND sequence_nr   <= @maxSeq
		        AND snapshot_ts   >= @minTs
		        AND snapshot_ts   <= @maxTs`,
		Params: map[string]any{
			"pid":    persistenceId,
			"minSeq": int64(criteria.MinSequenceNr),
			"maxSeq": maxSeqNr,
			"minTs":  criteria.MinTimestamp,
			"maxTs":  maxTs,
		},
	}
	_, err := s.client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return fmt.Errorf("spannersnapshot: delete range for %q: %w", persistenceId, err)
	}
	return nil
}

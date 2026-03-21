/*
 * journal.go
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
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sopranoworks/gekka/persistence"
	"google.golang.org/api/iterator"
)

// SpannerJournal implements persistence.Journal on top of Cloud Spanner.
//
// Events are stored in the `journal` table (see JournalDDL).  Writes use
// InsertOrUpdate mutations for idempotency; reads use read-only transactions
// with SQL queries.
//
// Create the schema once before first use:
//
//	spannerstore.CreateSchema(ctx, adminClient, databaseName)
type SpannerJournal struct {
	client *spanner.Client
	codec  PayloadCodec
}

// NewSpannerJournal creates a SpannerJournal.
//
//   - client — an open *spanner.Client pointing at the target database
//   - codec  — payload serialiser; all event types must be registered
func NewSpannerJournal(client *spanner.Client, codec PayloadCodec) *SpannerJournal {
	return &SpannerJournal{client: client, codec: codec}
}

// AsyncWriteMessages writes a batch of events using InsertOrUpdate mutations
// (idempotent: re-writing the same (persistenceId, sequenceNr) is a no-op
// because the payload is identical).
func (j *SpannerJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	if len(messages) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	muts := make([]*spanner.Mutation, 0, len(messages))
	for _, msg := range messages {
		manifest, data, err := j.codec.Encode(msg.Payload)
		if err != nil {
			return fmt.Errorf("spannerjournal: encode %s/%d: %w", msg.PersistenceID, msg.SequenceNr, err)
		}
		muts = append(muts, spanner.InsertOrUpdate("journal",
			[]string{"persistence_id", "sequence_nr", "payload", "manifest",
				"sender_path", "deleted", "written_at", "tags"},
			[]any{
				msg.PersistenceID,
				int64(msg.SequenceNr),
				data,
				manifest,
				msg.SenderPath,
				msg.Deleted,
				now,
				strings.Join(msg.Tags, ","),
			},
		))
	}
	_, err := j.client.Apply(ctx, muts)
	if err != nil {
		return fmt.Errorf("spannerjournal: apply mutations: %w", err)
	}
	return nil
}

// ReadHighestSequenceNr returns the highest stored sequence number for
// persistenceId that is >= fromSequenceNr.  Returns 0 when no rows exist.
func (j *SpannerJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	stmt := spanner.Statement{
		SQL: `SELECT COALESCE(MAX(sequence_nr), -1) AS highest
		      FROM journal
		      WHERE persistence_id = @pid
		        AND sequence_nr >= @from`,
		Params: map[string]any{
			"pid":  persistenceId,
			"from": int64(fromSequenceNr),
		},
	}
	var highest int64
	err := j.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		return row.Column(0, &highest)
	})
	if err != nil {
		return 0, fmt.Errorf("spannerjournal: read highest seq nr for %q: %w", persistenceId, err)
	}
	if highest < 0 {
		return 0, nil
	}
	return uint64(highest), nil
}

// ReplayMessages queries events for persistenceId in [fromSequenceNr, toSequenceNr]
// and calls callback for each in sequence order.  Pass max=0 for unlimited.
func (j *SpannerJournal) ReplayMessages(
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
	stmt := spanner.Statement{
		SQL: `SELECT persistence_id, sequence_nr, payload, manifest, sender_path
		      FROM journal
		      WHERE persistence_id = @pid
		        AND sequence_nr >= @from
		        AND sequence_nr <= @to
		        AND deleted = false
		      ORDER BY sequence_nr ASC
		      LIMIT @lim`,
		Params: map[string]any{
			"pid":  persistenceId,
			"from": int64(fromSequenceNr),
			"to":   int64(toSequenceNr),
			"lim":  limit,
		},
	}
	iter := j.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("spannerjournal: replay query for %q: %w", persistenceId, err)
		}
		var (
			pid        string
			seqNr      int64
			rawPayload []byte
			manifest   string
			senderPath string
		)
		if err := row.Columns(&pid, &seqNr, &rawPayload, &manifest, &senderPath); err != nil {
			return fmt.Errorf("spannerjournal: scan row for %q: %w", persistenceId, err)
		}
		payload, err := j.codec.Decode(manifest, rawPayload)
		if err != nil {
			return fmt.Errorf("spannerjournal: decode %q/%d (manifest=%q): %w",
				persistenceId, seqNr, manifest, err)
		}
		callback(persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    uint64(seqNr),
			Payload:       payload,
			SenderPath:    senderPath,
		})
	}
	return nil
}

// AsyncDeleteMessagesTo deletes all events for persistenceId with sequence
// numbers <= toSequenceNr using a partitioned DML statement.
func (j *SpannerJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	stmt := spanner.Statement{
		SQL: `DELETE FROM journal
		      WHERE persistence_id = @pid
		        AND sequence_nr <= @to`,
		Params: map[string]any{
			"pid": persistenceId,
			"to":  int64(toSequenceNr),
		},
	}
	_, err := j.client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return fmt.Errorf("spannerjournal: delete for %q up to %d: %w", persistenceId, toSequenceNr, err)
	}
	return nil
}

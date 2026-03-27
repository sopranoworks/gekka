/*
 * query_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"google.golang.org/api/iterator"
)

// SpannerReadJournal implements query.ReadJournal for Cloud Spanner.
type SpannerReadJournal struct {
	client          *spanner.Client
	codec           PayloadCodec
	refreshInterval time.Duration
}

// NewSpannerReadJournal creates a new SpannerReadJournal.
func NewSpannerReadJournal(client *spanner.Client, codec PayloadCodec) *SpannerReadJournal {
	return &SpannerReadJournal{
		client:          client,
		codec:           codec,
		refreshInterval: 500 * time.Millisecond,
	}
}

func (j *SpannerReadJournal) IsReadJournal() {}

// EventsByPersistenceId provides a stream of events for a specific persistent actor.
func (j *SpannerReadJournal) EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		for {
			if toSequenceNr > 0 && fromSequenceNr > toSequenceNr {
				return query.EventEnvelope{}, false, nil
			}

			stmt := spanner.Statement{
				SQL: `SELECT persistence_id, sequence_nr, payload, manifest, written_at, commit_timestamp
				      FROM journal
				      WHERE persistence_id = @pid
				        AND sequence_nr = @seq
				        AND deleted = false`,
				Params: map[string]any{
					"pid": persistenceId,
					"seq": fromSequenceNr,
				},
			}

			iter := j.client.Single().Query(context.Background(), stmt)
			row, err := iter.Next()
			iter.Stop()

			if err == nil {
				var (
					pid       string
					seqNr     int64
					data      []byte
					manifest  string
					writtenAt int64
					commitTs  time.Time
				)
				if err := row.Columns(&pid, &seqNr, &data, &manifest, &writtenAt, &commitTs); err != nil {
					return query.EventEnvelope{}, false, err
				}

				payload, err := j.codec.Decode(manifest, data)
				if err != nil {
					return query.EventEnvelope{}, false, err
				}

				fromSequenceNr++
				return query.EventEnvelope{
					Offset:        query.SequenceOffset(seqNr),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr),
					Event:         payload,
					Timestamp:     writtenAt,
				}, true, nil
			}

			if err != iterator.Done {
				return query.EventEnvelope{}, false, err
			}

			// Not found
			if toSequenceNr > 0 {
				// Non-live stream or we reached the limit
				// Check if there are higher sequence numbers to be sure
				highest, err := j.readHighest(persistenceId)
				if err != nil {
					return query.EventEnvelope{}, false, err
				}
				if int64(highest) < fromSequenceNr {
					return query.EventEnvelope{}, false, nil
				}
			}

			time.Sleep(j.refreshInterval)
		}
	})
}

// CurrentEventsByPersistenceId provides a non-streaming query for current events.
func (j *SpannerReadJournal) CurrentEventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	// For simplicity, we can use the same logic but without polling.
	// We'll wrap it to complete when no more events are found.
	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		if toSequenceNr > 0 && fromSequenceNr > toSequenceNr {
			return query.EventEnvelope{}, false, nil
		}

		stmt := spanner.Statement{
			SQL: `SELECT persistence_id, sequence_nr, payload, manifest, written_at, commit_timestamp
			      FROM journal
			      WHERE persistence_id = @pid
			        AND sequence_nr = @seq
			        AND deleted = false`,
			Params: map[string]any{
				"pid": persistenceId,
				"seq": fromSequenceNr,
			},
		}

		iter := j.client.Single().Query(context.Background(), stmt)
		row, err := iter.Next()
		iter.Stop()

		if err == iterator.Done {
			return query.EventEnvelope{}, false, nil
		}
		if err != nil {
			return query.EventEnvelope{}, false, err
		}

		var (
			pid       string
			seqNr     int64
			data      []byte
			manifest  string
			writtenAt int64
			commitTs  time.Time
		)
		if err := row.Columns(&pid, &seqNr, &data, &manifest, &writtenAt, &commitTs); err != nil {
			return query.EventEnvelope{}, false, err
		}

		payload, err := j.codec.Decode(manifest, data)
		if err != nil {
			return query.EventEnvelope{}, false, err
		}

		fromSequenceNr++
		return query.EventEnvelope{
			Offset:        query.SequenceOffset(seqNr),
			PersistenceID: pid,
			SequenceNr:    uint64(seqNr),
			Event:         payload,
			Timestamp:     writtenAt,
		}, true, nil
	})
}

// EventsByTag provides a stream of events matching a tag.
func (j *SpannerReadJournal) EventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	var lastTimestamp time.Time
	if to, ok := offset.(query.TimeOffset); ok {
		lastTimestamp = time.Unix(0, int64(to))
	}

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		for {
			// Query event_tag table to find the next event
			stmt := spanner.Statement{
				SQL: `SELECT t.persistence_id, t.sequence_nr, t.commit_timestamp, j.payload, j.manifest, j.written_at
				      FROM event_tag AS t
				      JOIN journal AS j ON t.persistence_id = j.persistence_id AND t.sequence_nr = j.sequence_nr
				      WHERE t.tag = @tag
				        AND t.commit_timestamp > @ts
				      ORDER BY t.commit_timestamp ASC
				      LIMIT 1`,
				Params: map[string]any{
					"tag": tag,
					"ts":  lastTimestamp,
				},
			}

			iter := j.client.Single().Query(context.Background(), stmt)
			row, err := iter.Next()
			iter.Stop()

			if err == nil {
				var (
					pid       string
					seqNr     int64
					commitTs  time.Time
					data      []byte
					manifest  string
					writtenAt int64
				)
				if err := row.Columns(&pid, &seqNr, &commitTs, &data, &manifest, &writtenAt); err != nil {
					return query.EventEnvelope{}, false, err
				}

				payload, err := j.codec.Decode(manifest, data)
				if err != nil {
					return query.EventEnvelope{}, false, err
				}

				lastTimestamp = commitTs
				return query.EventEnvelope{
					Offset:        query.TimeOffset(commitTs.UnixNano()),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr),
					Event:         payload,
					Timestamp:     writtenAt,
				}, true, nil
			}

			if err != iterator.Done {
				return query.EventEnvelope{}, false, err
			}

			time.Sleep(j.refreshInterval)
		}
	})
}

func (j *SpannerReadJournal) readHighest(persistenceId string) (uint64, error) {
	stmt := spanner.Statement{
		SQL: `SELECT COALESCE(MAX(sequence_nr), 0)
		      FROM journal
		      WHERE persistence_id = @pid`,
		Params: map[string]any{
			"pid": persistenceId,
		},
	}
	var highest int64
	err := j.client.Single().Query(context.Background(), stmt).Do(func(row *spanner.Row) error {
		return row.Column(0, &highest)
	})
	return uint64(highest), err
}

func (j *SpannerReadJournal) CurrentPersistenceIds() stream.Source[string, stream.NotUsed] {
	var iter *spanner.RowIterator
	return stream.FromIteratorFunc(func() (string, bool, error) {
		if iter == nil {
			stmt := spanner.Statement{
				SQL: `SELECT DISTINCT persistence_id FROM journal`,
			}
			iter = j.client.Single().Query(context.Background(), stmt)
		}

		row, err := iter.Next()
		if err == iterator.Done {
			iter.Stop()
			return "", false, nil
		}
		if err != nil {
			iter.Stop()
			return "", false, err
		}

		var pid string
		if err := row.Columns(&pid); err != nil {
			iter.Stop()
			return "", false, err
		}

		return pid, true, nil
	})
}

func (j *SpannerReadJournal) PersistenceIds() stream.Source[string, stream.NotUsed] {
	seen := make(map[string]struct{})
	var iter *spanner.RowIterator

	return stream.FromIteratorFunc(func() (string, bool, error) {
		for {
			if iter == nil {
				stmt := spanner.Statement{
					SQL: `SELECT DISTINCT persistence_id FROM journal`,
				}
				iter = j.client.Single().Query(context.Background(), stmt)
			}

			row, err := iter.Next()
			if err == iterator.Done {
				iter.Stop()
				iter = nil
				time.Sleep(j.refreshInterval)
				continue
			}
			if err != nil {
				iter.Stop()
				iter = nil
				return "", false, err
			}

			var pid string
			if err := row.Columns(&pid); err != nil {
				iter.Stop()
				iter = nil
				return "", false, err
			}

			if _, exists := seen[pid]; !exists {
				seen[pid] = struct{}{}
				return pid, true, nil
			}
			// if already seen, immediately loop and pull next row
		}
	})
}

var (
	_ query.EventsByPersistenceIdQuery        = (*SpannerReadJournal)(nil)
	_ query.EventsByTagQuery                  = (*SpannerReadJournal)(nil)
	_ query.CurrentEventsByPersistenceIdQuery = (*SpannerReadJournal)(nil)
	_ query.PersistenceIdsQuery               = (*SpannerReadJournal)(nil)
	_ query.CurrentPersistenceIdsQuery        = (*SpannerReadJournal)(nil)
)

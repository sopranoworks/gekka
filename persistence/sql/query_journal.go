/*
 * query_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

import (
	"context"
	"database/sql"
	"time"

	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
)

// SQLReadJournal implements query.ReadJournal for SQL databases.
type SQLReadJournal struct {
	db              *sql.DB
	dialect         Dialect
	codec           PayloadCodec
	table           string
	refreshInterval time.Duration
}

// NewSQLReadJournal creates a new SQLReadJournal.
func NewSQLReadJournal(db *sql.DB, dialect Dialect, codec PayloadCodec, config Config) *SQLReadJournal {
	return &SQLReadJournal{
		db:              db,
		dialect:         dialect,
		codec:           codec,
		table:           config.journalTable(),
		refreshInterval: 500 * time.Millisecond,
	}
}

func (j *SQLReadJournal) IsReadJournal() {}

// EventsByPersistenceId provides a stream of events for a specific persistent actor.
func (j *SQLReadJournal) EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		for {
			if toSequenceNr > 0 && fromSequenceNr > toSequenceNr {
				return query.EventEnvelope{}, false, nil
			}

			ctx := context.Background()
			rows, err := j.db.QueryContext(ctx,
				j.dialect.JournalReplaySQL(j.table),
				persistenceId,
				fromSequenceNr,
				fromSequenceNr, // fetch exactly one
				1,
			)
			if err != nil {
				return query.EventEnvelope{}, false, err
			}

			found := false
			var (
				pid        string
				seqNr      int64
				rawPayload []byte
				manifest   string
				senderPath string
				createdAt  int64
				ordering   int64
			)

			if rows.Next() {
				if err := rows.Scan(&pid, &seqNr, &rawPayload, &manifest, &senderPath, &createdAt, &ordering); err == nil {
					found = true
				} else {
					rows.Close()
					return query.EventEnvelope{}, false, err
				}
			}
			rows.Close()

			if found {
				payload, err := j.codec.Decode(manifest, rawPayload)
				if err != nil {
					return query.EventEnvelope{}, false, err
				}

				fromSequenceNr++
				return query.EventEnvelope{
					Offset:        query.SequenceOffset(ordering),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr),
					Event:         payload,
					Timestamp:     createdAt,
				}, true, nil
			}

			// If toSequenceNr was specified and we found nothing, we might be at the end.
			// But for live streams (toSequenceNr <= 0), we poll.
			if toSequenceNr > 0 {
				return query.EventEnvelope{}, false, nil
			}

			time.Sleep(j.refreshInterval)
		}
	})
}

// EventsByTag provides a stream of events matching a tag.
func (j *SQLReadJournal) EventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	var currentOrdering int64
	if so, ok := offset.(query.SequenceOffset); ok {
		currentOrdering = int64(so)
	}

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		for {
			ctx := context.Background()
			rows, err := j.db.QueryContext(ctx,
				j.dialect.JournalEventsByTagSQL(j.table),
				tag,
				currentOrdering,
				1,
			)
			if err != nil {
				return query.EventEnvelope{}, false, err
			}

			found := false
			var (
				pid        string
				seqNr      int64
				rawPayload []byte
				manifest   string
				senderPath string
				createdAt  int64
				ordering   int64
			)

			if rows.Next() {
				if err := rows.Scan(&pid, &seqNr, &rawPayload, &manifest, &senderPath, &createdAt, &ordering); err == nil {
					found = true
				} else {
					rows.Close()
					return query.EventEnvelope{}, false, err
				}
			}
			rows.Close()

			if found {
				payload, err := j.codec.Decode(manifest, rawPayload)
				if err != nil {
					return query.EventEnvelope{}, false, err
				}

				currentOrdering = ordering
				return query.EventEnvelope{
					Offset:        query.SequenceOffset(ordering),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr),
					Event:         payload,
					Timestamp:     createdAt,
				}, true, nil
			}

			time.Sleep(j.refreshInterval)
		}
	})
}

func (j *SQLReadJournal) CurrentPersistenceIds() stream.Source[string, stream.NotUsed] {
	return stream.FromIteratorFunc(func() (string, bool, error) {
		// Just run it once and buffer the results in memory? No we need an iterator
		// But stream.FromIteratorFunc evaluates per element! We can't query db per element
		// Instead we use a local context variable and execute query on first iteration
		var rows *sql.Rows
		var queryErr error
		var done bool

		if !done && rows == nil {
			ctx := context.Background()
			rows, queryErr = j.db.QueryContext(ctx, j.dialect.JournalCurrentPersistenceIdsSQL(j.table))
			if queryErr != nil {
				return "", false, queryErr
			}
		}

		if rows != nil && rows.Next() {
			var pid string
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				return "", false, err
			}
			return pid, true, nil
		}

		if rows != nil {
			queryErr = rows.Err()
			rows.Close()
			rows = nil
		}
		done = true

		if queryErr != nil {
			return "", false, queryErr
		}
		return "", false, nil
	})
}

func (j *SQLReadJournal) PersistenceIds() stream.Source[string, stream.NotUsed] {
	var currentOrdering int64
	var rows *sql.Rows
	var queryErr error

	return stream.FromIteratorFunc(func() (string, bool, error) {
		for {
			if rows == nil && queryErr == nil {
				ctx := context.Background()
				rows, queryErr = j.db.QueryContext(ctx,
					j.dialect.JournalPersistenceIdsSQL(j.table),
					currentOrdering,
					100, // fetch batches of 100 at a time
				)
				if queryErr != nil {
					return "", false, queryErr
				}
			}

			if rows != nil && rows.Next() {
				var pid string
				var maxOrd int64
				if err := rows.Scan(&pid, &maxOrd); err != nil {
					rows.Close()
					rows = nil
					return "", false, err
				}
				// We don't advance currentOrdering until we process all locally
				// Wait! If we fetch 100 maxOrds, each row has a different maxOrd
				// To paginate correctly, currentOrdering MUST be strictly increasing for the query?
				// But GROUP BY persistence_id orders by max_ord ASC!
				// So we just advance currentOrdering to the highest max_ord we've yielded!
				if maxOrd > currentOrdering {
					currentOrdering = maxOrd
				}
				return pid, true, nil
			}

			if rows != nil {
				queryErr = rows.Err()
				rows.Close()
				rows = nil
			}

			if queryErr != nil {
				err := queryErr
				queryErr = nil // reset for polling
				return "", false, err
			}

			// We reached the end of the batch; wait and poll again
			time.Sleep(j.refreshInterval)
		}
	})
}

var (
	_ query.ReadJournal                = (*SQLReadJournal)(nil)
	_ query.EventsByPersistenceIdQuery = (*SQLReadJournal)(nil)
	_ query.EventsByTagQuery           = (*SQLReadJournal)(nil)
	_ query.PersistenceIdsQuery        = (*SQLReadJournal)(nil)
	_ query.CurrentPersistenceIdsQuery = (*SQLReadJournal)(nil)
)

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

var _ query.ReadJournal = (*SQLReadJournal)(nil)

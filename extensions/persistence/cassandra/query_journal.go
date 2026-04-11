/*
 * query_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
)

// CassandraReadJournal implements query.ReadJournal for Apache Cassandra.
// It supports both live (unbounded) and current (bounded) event streams,
// tag-based queries using timebucket partitioning, and persistence ID enumeration.
type CassandraReadJournal struct {
	session         *gocql.Session
	keyspace        string
	journalTable    string
	metadataTable   string
	tagTable        string
	scanningTable   string
	targetPartSz    int64
	bucketSize      BucketSize
	codec           PayloadCodec
	refreshInterval time.Duration
}

// NewCassandraReadJournal constructs a CassandraReadJournal from a live session and config.
func NewCassandraReadJournal(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraReadJournal {
	return &CassandraReadJournal{
		session:         session,
		keyspace:        cfg.JournalKeyspace,
		journalTable:    cfg.JournalTable,
		metadataTable:   cfg.MetadataTable,
		tagTable:        cfg.TagTable,
		scanningTable:   cfg.ScanningTable,
		targetPartSz:    cfg.TargetPartitionSize,
		bucketSize:      cfg.BucketSize,
		codec:           codec,
		refreshInterval: 3 * time.Second,
	}
}

// IsReadJournal marks CassandraReadJournal as a ReadJournal.
func (j *CassandraReadJournal) IsReadJournal() {}

// readMaxPartNr reads the current partition number for pid from the metadata table.
func (j *CassandraReadJournal) readMaxPartNr(pid string) (int64, error) {
	q := fmt.Sprintf(
		`SELECT partition_nr FROM %s.%s WHERE persistence_id = ?`,
		j.keyspace, j.metadataTable,
	)
	var partNr int64
	err := j.session.Query(q, pid).Scan(&partNr)
	if err != nil {
		if err.Error() == "not found" {
			return 0, nil
		}
		return 0, fmt.Errorf("cassandrareadjournal: readMaxPartNr %q: %w", pid, err)
	}
	return partNr, nil
}

// readHighest walks partitions backwards to find the highest stored sequence number for pid.
func (j *CassandraReadJournal) readHighest(pid string) (int64, error) {
	maxPartNr, err := j.readMaxPartNr(pid)
	if err != nil {
		return 0, err
	}

	q := fmt.Sprintf(
		`SELECT sequence_nr FROM %s.%s
		 WHERE persistence_id = ? AND partition_nr = ?
		 ORDER BY sequence_nr DESC LIMIT 1`,
		j.keyspace, j.journalTable,
	)

	for partNr := maxPartNr; partNr >= 0; partNr-- {
		var seqNr int64
		err := j.session.Query(q, pid, partNr).Scan(&seqNr)
		if err != nil {
			if err.Error() == "not found" {
				if partNr == 0 {
					break
				}
				continue
			}
			return 0, fmt.Errorf("cassandrareadjournal: readHighest %q: %w", pid, err)
		}
		return seqNr, nil
	}
	return 0, nil
}

// scanPartitions scans Cassandra partitions for pid from fromSeq to toSeq
// and sends EventEnvelopes to the results channel.  It always closes results on return.
// When live is true it polls indefinitely for new events; otherwise it returns
// once all currently stored events have been emitted.
func (j *CassandraReadJournal) scanPartitions(
	pid string,
	fromSeq, toSeq int64,
	live bool,
	results chan<- query.EventEnvelope,
	errCh chan<- error,
) {
	defer close(results)

	q := fmt.Sprintf(
		`SELECT sequence_nr, event, ser_manifest, event_manifest, tags
		 FROM %s.%s
		 WHERE persistence_id = ?
		   AND partition_nr = ?
		   AND sequence_nr >= ?
		   AND sequence_nr <= ?
		 ORDER BY sequence_nr ASC`,
		j.keyspace, j.journalTable,
	)

	curSeq := fromSeq

	for {
		maxPartNr, err := j.readMaxPartNr(pid)
		if err != nil {
			errCh <- err
			return
		}

		effectiveToSeq := toSeq
		if live && toSeq <= 0 {
			// For live streams with no upper bound, use a very large number
			// so we scan all partitions; we'll poll when nothing new is found.
			effectiveToSeq = (maxPartNr + 1) * j.targetPartSz
		}

		startPart := curSeq / j.targetPartSz
		foundAny := false

		for partNr := startPart; partNr <= maxPartNr; partNr++ {
			iter := j.session.Query(q, pid, partNr, curSeq, effectiveToSeq).Iter()

			var (
				seqNr         int64
				eventData     []byte
				serManifest   string
				eventManifest string
				tags          []string
			)
			for iter.Scan(&seqNr, &eventData, &serManifest, &eventManifest, &tags) {
				payload, decErr := j.codec.Decode(serManifest, eventData)
				if decErr != nil {
					_ = iter.Close()
					errCh <- fmt.Errorf("cassandrareadjournal: decode %q seq %d: %w", pid, seqNr, decErr)
					return
				}
				env := query.EventEnvelope{
					Offset:        query.SequenceOffset(seqNr),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr), //nolint:gosec
					Event:         payload,
					Timestamp:     time.Now().UnixNano(),
				}
				results <- env
				curSeq = seqNr + 1
				foundAny = true
			}
			if err := iter.Close(); err != nil {
				errCh <- fmt.Errorf("cassandrareadjournal: scan %q partition %d: %w", pid, partNr, err)
				return
			}
		}

		if !live {
			// Finite stream — done after one full scan.
			return
		}

		// Live stream — poll when there are no new events.
		if !foundAny {
			time.Sleep(j.refreshInterval)
		}
	}
}

// EventsByPersistenceId provides a live stream of events for a persistent actor.
// The stream never completes: as new events are appended they are emitted.
func (j *CassandraReadJournal) EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	live := toSequenceNr <= 0
	results := make(chan query.EventEnvelope, 64)
	errCh := make(chan error, 1)

	go j.scanPartitions(persistenceId, fromSequenceNr, toSequenceNr, live, results, errCh)

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		select {
		case err := <-errCh:
			return query.EventEnvelope{}, false, err
		case env, ok := <-results:
			if !ok {
				// Check for a deferred error after channel close.
				select {
				case err := <-errCh:
					return query.EventEnvelope{}, false, err
				default:
				}
				return query.EventEnvelope{}, false, nil
			}
			return env, true, nil
		}
	})
}

// CurrentEventsByPersistenceId provides a bounded stream of events up to the
// highest sequence number currently stored.  The source completes once all
// current events have been emitted.
func (j *CassandraReadJournal) CurrentEventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	effectiveTo := toSequenceNr
	if effectiveTo <= 0 {
		// Resolve the current highest sequence number as the effective upper bound.
		highest, err := j.readHighest(persistenceId)
		if err == nil {
			effectiveTo = highest
		}
	}

	results := make(chan query.EventEnvelope, 64)
	errCh := make(chan error, 1)

	go j.scanPartitions(persistenceId, fromSequenceNr, effectiveTo, false, results, errCh)

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		select {
		case err := <-errCh:
			return query.EventEnvelope{}, false, err
		case env, ok := <-results:
			if !ok {
				select {
				case err := <-errCh:
					return query.EventEnvelope{}, false, err
				default:
				}
				return query.EventEnvelope{}, false, nil
			}
			return env, true, nil
		}
	})
}

// eventsByTagInternal is the shared implementation for EventsByTag and CurrentEventsByTag.
// When live is true the stream polls indefinitely; otherwise it completes when
// the current bucket is exhausted.
func (j *CassandraReadJournal) eventsByTagInternal(tag string, offset query.Offset, live bool) stream.Source[query.EventEnvelope, stream.NotUsed] {
	// Determine start time from offset.
	var startTime time.Time
	if to, ok := offset.(query.TimeOffset); ok {
		startTime = time.Unix(0, int64(to))
	} else {
		// Default to year 2000.
		startTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	results := make(chan query.EventEnvelope, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(results)

		q := fmt.Sprintf(
			`SELECT persistence_id, sequence_nr, timestamp, event, ser_manifest, event_manifest
			 FROM %s.%s
			 WHERE tag_name = ?
			   AND timebucket = ?
			   AND timestamp > ?
			 ORDER BY timestamp ASC`,
			j.keyspace, j.tagTable,
		)

		curBucket := Timebucket(startTime, j.bucketSize)
		nowBucket := Timebucket(time.Now(), j.bucketSize)
		lastTimestamp := gocql.MinTimeUUID(startTime)

		for {
			foundAny := false
			curNowBucket := Timebucket(time.Now(), j.bucketSize)

			for bucket := curBucket; bucket <= curNowBucket; bucket++ {
				iter := j.session.Query(q, tag, bucket, lastTimestamp).Iter()

				var (
					pid           string
					seqNr         int64
					ts            gocql.UUID
					eventData     []byte
					serManifest   string
					eventManifest string
				)
				for iter.Scan(&pid, &seqNr, &ts, &eventData, &serManifest, &eventManifest) {
					payload, decErr := j.codec.Decode(serManifest, eventData)
					if decErr != nil {
						_ = iter.Close()
						errCh <- fmt.Errorf("cassandrareadjournal: decode tag %q pid %q seq %d: %w", tag, pid, seqNr, decErr)
						return
					}
					tsTime := ts.Time()
					env := query.EventEnvelope{
						Offset:        query.TimeOffset(tsTime.UnixNano()),
						PersistenceID: pid,
						SequenceNr:    uint64(seqNr), //nolint:gosec
						Event:         payload,
						Timestamp:     tsTime.UnixNano(),
					}
					results <- env
					lastTimestamp = ts
					curBucket = bucket
					foundAny = true
				}
				if err := iter.Close(); err != nil {
					errCh <- fmt.Errorf("cassandrareadjournal: tag scan %q bucket %d: %w", tag, bucket, err)
					return
				}
			}

			if !live {
				// Non-live: complete once we've scanned up to the current bucket.
				_ = nowBucket // used as initial capture only; real bound is curNowBucket
				return
			}

			// Live stream — poll when no new events found in the current bucket.
			if !foundAny {
				time.Sleep(j.refreshInterval)
			}
		}
	}()

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		select {
		case err := <-errCh:
			return query.EventEnvelope{}, false, err
		case env, ok := <-results:
			if !ok {
				select {
				case err := <-errCh:
					return query.EventEnvelope{}, false, err
				default:
				}
				return query.EventEnvelope{}, false, nil
			}
			return env, true, nil
		}
	})
}

// EventsByTag provides a live stream of events matching the given tag across
// all persistence IDs, starting from offset.
func (j *CassandraReadJournal) EventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return j.eventsByTagInternal(tag, offset, true)
}

// CurrentEventsByTag provides a bounded stream of events matching the given
// tag up to the current point in time.
func (j *CassandraReadJournal) CurrentEventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return j.eventsByTagInternal(tag, offset, false)
}

// CurrentPersistenceIds returns a stream of all known persistence IDs,
// completing once the currently stored set is exhausted.
func (j *CassandraReadJournal) CurrentPersistenceIds() stream.Source[string, stream.NotUsed] {
	results := make(chan string, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(results)

		q := fmt.Sprintf(
			`SELECT persistence_id FROM %s.%s`,
			j.keyspace, j.metadataTable,
		)
		iter := j.session.Query(q).Iter()
		var pid string
		for iter.Scan(&pid) {
			results <- pid
		}
		if err := iter.Close(); err != nil {
			errCh <- fmt.Errorf("cassandrareadjournal: CurrentPersistenceIds: %w", err)
		}
	}()

	return stream.FromIteratorFunc(func() (string, bool, error) {
		select {
		case err := <-errCh:
			return "", false, err
		case pid, ok := <-results:
			if !ok {
				select {
				case err := <-errCh:
					return "", false, err
				default:
				}
				return "", false, nil
			}
			return pid, true, nil
		}
	})
}

// PersistenceIds returns a live stream of all unique persistence IDs, emitting
// new IDs as they appear.
func (j *CassandraReadJournal) PersistenceIds() stream.Source[string, stream.NotUsed] {
	results := make(chan string, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(results)

		seen := make(map[string]struct{})
		q := fmt.Sprintf(
			`SELECT persistence_id FROM %s.%s`,
			j.keyspace, j.metadataTable,
		)

		for {
			iter := j.session.Query(q).Iter()
			var pid string
			foundNew := false
			for iter.Scan(&pid) {
				if _, exists := seen[pid]; !exists {
					seen[pid] = struct{}{}
					results <- pid
					foundNew = true
				}
			}
			if err := iter.Close(); err != nil {
				errCh <- fmt.Errorf("cassandrareadjournal: PersistenceIds: %w", err)
				return
			}
			_ = foundNew
			time.Sleep(j.refreshInterval)
		}
	}()

	return stream.FromIteratorFunc(func() (string, bool, error) {
		select {
		case err := <-errCh:
			return "", false, err
		case pid, ok := <-results:
			if !ok {
				select {
				case err := <-errCh:
					return "", false, err
				default:
				}
				return "", false, nil
			}
			return pid, true, nil
		}
	})
}

// Compile-time checks that CassandraReadJournal satisfies all required interfaces.
var (
	_ query.ReadJournal                       = (*CassandraReadJournal)(nil)
	_ query.EventsByPersistenceIdQuery        = (*CassandraReadJournal)(nil)
	_ query.CurrentEventsByPersistenceIdQuery = (*CassandraReadJournal)(nil)
	_ query.EventsByTagQuery                  = (*CassandraReadJournal)(nil)
	_ query.PersistenceIdsQuery               = (*CassandraReadJournal)(nil)
	_ query.CurrentPersistenceIdsQuery        = (*CassandraReadJournal)(nil)
)

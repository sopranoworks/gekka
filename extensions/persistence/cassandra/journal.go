/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraJournal implements persistence.Journal backed by Apache Cassandra.
// It follows the Pekko Cassandra Plugin partition-bucketing scheme: each
// persistence ID is divided into fixed-size partitions of TargetPartitionSize
// events so that no single Cassandra partition grows unboundedly.
type CassandraJournal struct {
	session       *gocql.Session
	keyspace      string
	table         string // "messages"
	metadataTable string // "metadata"
	targetPartSz  int64  // 500000
	codec         PayloadCodec
	writerUUID    string // uuid.New() at startup
	bucketSize    BucketSize
	tagTable      string
	scanningTable string
	partCache     *lru.Cache[string, int64] // persistenceId -> currentPartitionNr
}

// NewCassandraJournal constructs a CassandraJournal from a live session and
// config.  The LRU cache is pre-allocated with capacity 10,000.
func NewCassandraJournal(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraJournal {
	cache, _ := lru.New[string, int64](10_000)
	return &CassandraJournal{
		session:       session,
		keyspace:      cfg.JournalKeyspace,
		table:         cfg.JournalTable,
		metadataTable: cfg.MetadataTable,
		targetPartSz:  cfg.TargetPartitionSize,
		codec:         codec,
		writerUUID:    uuid.New().String(),
		bucketSize:    cfg.BucketSize,
		tagTable:      cfg.TagTable,
		scanningTable: cfg.ScanningTable,
		partCache:     cache,
	}
}

// resolvePartitionNr returns the current partition number for pid.
// It consults the in-process LRU first and falls back to a SELECT from the
// metadata table.  If no row exists yet, 0 is cached and returned.
func (j *CassandraJournal) resolvePartitionNr(ctx context.Context, pid string) (int64, error) {
	if v, ok := j.partCache.Get(pid); ok {
		return v, nil
	}
	var partNr int64
	q := fmt.Sprintf(
		`SELECT partition_nr FROM %s.%s WHERE persistence_id = ?`,
		j.keyspace, j.metadataTable,
	)
	err := j.session.Query(q, pid).WithContext(ctx).Scan(&partNr)
	if err != nil {
		if err.Error() == "not found" {
			j.partCache.Add(pid, 0)
			return 0, nil
		}
		return 0, fmt.Errorf("cassandrajournal: resolvePartitionNr %q: %w", pid, err)
	}
	j.partCache.Add(pid, partNr)
	return partNr, nil
}

// bumpPartitionNr writes a new partition_nr into the metadata table and
// updates the LRU cache.
func (j *CassandraJournal) bumpPartitionNr(ctx context.Context, pid string, newPartNr int64) error {
	q := fmt.Sprintf(
		`INSERT INTO %s.%s (persistence_id, partition_nr) VALUES (?, ?)`,
		j.keyspace, j.metadataTable,
	)
	if err := j.session.Query(q, pid, newPartNr).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrajournal: bumpPartitionNr %q: %w", pid, err)
	}
	j.partCache.Add(pid, newPartNr)
	return nil
}

// AsyncWriteMessages persists a batch of events.
//
// For each event the method:
//  1. Resolves (or bumps) the partition number.
//  2. Encodes the payload via the codec.
//  3. INSERTs the row into the messages table.
//  4. Writes tag_views / tag_scanning rows for tagged events.
//  5. Ensures a metadata row exists for brand-new streams (seqNr==1 && partNr==0).
func (j *CassandraJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	for _, msg := range messages {
		pid := msg.PersistenceID
		seqNr := int64(msg.SequenceNr) //nolint:gosec // seq nr fits in int64

		partNr, err := j.resolvePartitionNr(ctx, pid)
		if err != nil {
			return err
		}

		// Roll to next partition when this sequence number would exceed the
		// current partition's capacity.
		if seqNr > (partNr+1)*j.targetPartSz {
			newPartNr := seqNr / j.targetPartSz
			if err := j.bumpPartitionNr(ctx, pid, newPartNr); err != nil {
				return err
			}
			partNr = newPartNr
		}

		manifest, data, err := j.codec.Encode(msg.Payload)
		if err != nil {
			return fmt.Errorf("cassandrajournal: encode %q seq %d: %w", pid, seqNr, err)
		}

		ts, err := gocql.RandomUUID()
		if err != nil {
			return fmt.Errorf("cassandrajournal: generate timeuuid: %w", err)
		}

		var tags []string
		if len(msg.Tags) > 0 {
			tags = msg.Tags
		}

		insertMsg := fmt.Sprintf(
			`INSERT INTO %s.%s
			 (persistence_id, partition_nr, sequence_nr, timestamp, writer_uuid,
			  ser_id, ser_manifest, event_manifest, event, tags)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			j.keyspace, j.table,
		)
		if err := j.session.Query(insertMsg,
			pid, partNr, seqNr, ts, j.writerUUID,
			0, manifest, msg.Manifest, data, tags,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("cassandrajournal: insert %q seq %d: %w", pid, seqNr, err)
		}

		// Ensure the metadata row exists for brand-new streams.
		if seqNr == 1 && partNr == 0 {
			ensureMeta := fmt.Sprintf(
				`INSERT INTO %s.%s (persistence_id, partition_nr) VALUES (?, ?) IF NOT EXISTS`,
				j.keyspace, j.metadataTable,
			)
			if err := j.session.Query(ensureMeta, pid, int64(0)).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("cassandrajournal: ensure metadata %q: %w", pid, err)
			}
		}

		// Tag index writes.
		if len(msg.Tags) > 0 {
			tb := Timebucket(time.Now(), j.bucketSize)

			insertTag := fmt.Sprintf(
				`INSERT INTO %s.%s
				 (tag_name, timebucket, timestamp, persistence_id, sequence_nr, partition_nr,
				  ser_id, ser_manifest, event_manifest, event)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				j.keyspace, j.tagTable,
			)
			insertScan := fmt.Sprintf(
				`INSERT INTO %s.%s (persistence_id, tag, sequence_nr) VALUES (?, ?, ?)`,
				j.keyspace, j.scanningTable,
			)

			for _, tag := range msg.Tags {
				if err := j.session.Query(insertTag,
					tag, tb, ts, pid, seqNr, partNr,
					0, manifest, msg.Manifest, data,
				).WithContext(ctx).Exec(); err != nil {
					return fmt.Errorf("cassandrajournal: insert tag_views %q tag %q: %w", pid, tag, err)
				}
				if err := j.session.Query(insertScan, pid, tag, seqNr).WithContext(ctx).Exec(); err != nil {
					return fmt.Errorf("cassandrajournal: insert tag_scanning %q tag %q: %w", pid, tag, err)
				}
			}
		}
	}
	return nil
}

// ReplayMessages reads events for pid in [fromSeqNr, toSeqNr] and invokes
// callback for each one, stopping after max events.
func (j *CassandraJournal) ReplayMessages(
	ctx context.Context,
	pid string,
	fromSeqNr, toSeqNr uint64,
	max uint64,
	callback func(persistence.PersistentRepr),
) error {
	maxPartNr, err := j.resolvePartitionNr(ctx, pid)
	if err != nil {
		return err
	}

	startPart := int64(fromSeqNr) / j.targetPartSz //nolint:gosec
	var count uint64

	for partNr := startPart; partNr <= maxPartNr; partNr++ {
		if count >= max {
			break
		}

		q := fmt.Sprintf(
			`SELECT sequence_nr, ser_manifest, event_manifest, event
			 FROM %s.%s
			 WHERE persistence_id = ?
			   AND partition_nr = ?
			   AND sequence_nr >= ?
			   AND sequence_nr <= ?
			 ORDER BY sequence_nr ASC`,
			j.keyspace, j.table,
		)
		iter := j.session.Query(q, pid, partNr, int64(fromSeqNr), int64(toSeqNr)). //nolint:gosec
											WithContext(ctx).Iter()

		var (
			seqNr         int64
			serManifest   string
			eventManifest string
			eventData     []byte
		)
		for iter.Scan(&seqNr, &serManifest, &eventManifest, &eventData) {
			if count >= max {
				break
			}
			payload, decErr := j.codec.Decode(serManifest, eventData)
			if decErr != nil {
				_ = iter.Close()
				return fmt.Errorf("cassandrajournal: decode %q seq %d: %w", pid, seqNr, decErr)
			}
			callback(persistence.PersistentRepr{
				PersistenceID: pid,
				SequenceNr:    uint64(seqNr), //nolint:gosec
				Payload:       payload,
				Manifest:      eventManifest,
			})
			count++
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("cassandrajournal: replay %q partition %d: %w", pid, partNr, err)
		}
	}
	return nil
}

// ReadHighestSequenceNr returns the highest sequence number stored for pid
// that is >= fromSeqNr.  It walks partitions backwards from the highest known
// partition number, returning the first match.
func (j *CassandraJournal) ReadHighestSequenceNr(ctx context.Context, pid string, fromSeqNr uint64) (uint64, error) {
	maxPartNr, err := j.resolvePartitionNr(ctx, pid)
	if err != nil {
		return 0, err
	}

	q := fmt.Sprintf(
		`SELECT sequence_nr FROM %s.%s
		 WHERE persistence_id = ? AND partition_nr = ?
		 ORDER BY sequence_nr DESC LIMIT 1`,
		j.keyspace, j.table,
	)

	for partNr := maxPartNr; partNr >= 0; partNr-- {
		var seqNr int64
		err := j.session.Query(q, pid, partNr).WithContext(ctx).Scan(&seqNr)
		if err != nil {
			if err.Error() == "not found" {
				if partNr == 0 {
					break
				}
				continue
			}
			return 0, fmt.Errorf("cassandrajournal: readHighestSeqNr %q: %w", pid, err)
		}
		if uint64(seqNr) >= fromSeqNr { //nolint:gosec
			return uint64(seqNr), nil //nolint:gosec
		}
		if partNr == 0 {
			break
		}
	}
	return 0, nil
}

// AsyncDeleteMessagesTo deletes all events for pid up to and including
// toSeqNr.  It iterates every partition in order, selecting rows to delete
// individually (required by Cassandra's PRIMARY KEY structure).
func (j *CassandraJournal) AsyncDeleteMessagesTo(ctx context.Context, pid string, toSeqNr uint64) error {
	maxPartNr, err := j.resolvePartitionNr(ctx, pid)
	if err != nil {
		return err
	}

	selectQ := fmt.Sprintf(
		`SELECT sequence_nr, timestamp FROM %s.%s
		 WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr <= ?`,
		j.keyspace, j.table,
	)
	deleteQ := fmt.Sprintf(
		`DELETE FROM %s.%s
		 WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr = ? AND timestamp = ?`,
		j.keyspace, j.table,
	)

	for partNr := int64(0); partNr <= maxPartNr; partNr++ {
		iter := j.session.Query(selectQ, pid, partNr, int64(toSeqNr)). //nolint:gosec
										WithContext(ctx).Iter()
		var (
			seqNr int64
			ts    gocql.UUID
		)
		for iter.Scan(&seqNr, &ts) {
			if err := j.session.Query(deleteQ, pid, partNr, seqNr, ts).WithContext(ctx).Exec(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("cassandrajournal: delete %q seq %d: %w", pid, seqNr, err)
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("cassandrajournal: delete scan %q partition %d: %w", pid, partNr, err)
		}
	}
	return nil
}

// Compile-time check that CassandraJournal satisfies persistence.Journal.
var _ persistence.Journal = (*CassandraJournal)(nil)

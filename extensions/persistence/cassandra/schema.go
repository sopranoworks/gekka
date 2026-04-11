/*
 * schema.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"

	"github.com/gocql/gocql"
)

// CreateSchema creates all required Cassandra keyspaces and tables according to
// the flags in cfg.  It is idempotent — all statements use IF NOT EXISTS.
func CreateSchema(session *gocql.Session, cfg *CassandraConfig) error {
	if cfg.KeyspaceAutocreate {
		if err := createKeyspace(session, cfg.JournalKeyspace, cfg.ReplicationStrategy, cfg.ReplicationFactor); err != nil {
			return err
		}
	}
	if cfg.SnapshotAutoKeyspace {
		if err := createKeyspace(session, cfg.SnapshotKeyspace, cfg.ReplicationStrategy, cfg.ReplicationFactor); err != nil {
			return err
		}
	}
	if cfg.TablesAutocreate {
		if err := createJournalTables(session, cfg); err != nil {
			return err
		}
		if err := createTagTables(session, cfg); err != nil {
			return err
		}
		if err := createStateTables(session, cfg); err != nil {
			return err
		}
	}
	if cfg.SnapshotAutoTables {
		if err := createSnapshotTables(session, cfg); err != nil {
			return err
		}
	}
	return nil
}

// createKeyspace creates a keyspace with the given replication settings.
func createKeyspace(session *gocql.Session, keyspace, strategy string, factor int) error {
	cql := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s
		 WITH replication = {'class': '%s', 'replication_factor': '%d'}
		 AND durable_writes = true`,
		keyspace, strategy, factor,
	)
	if err := session.Query(cql).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create keyspace %q: %w", keyspace, err)
	}
	return nil
}

// createJournalTables creates the messages and metadata tables in the journal
// keyspace.
func createJournalTables(session *gocql.Session, cfg *CassandraConfig) error {
	messagesCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  persistence_id  text,
  partition_nr    bigint,
  sequence_nr     bigint,
  timestamp       timeuuid,
  writer_uuid     text,
  ser_id          int,
  ser_manifest    text,
  event_manifest  text,
  event           blob,
  tags            set<text>,
  meta_ser_id     int,
  meta_ser_manifest text,
  meta            blob,
  PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp)
) WITH CLUSTERING ORDER BY (sequence_nr ASC, timestamp ASC)
  AND gc_grace_seconds = %d
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}`,
		cfg.JournalKeyspace, cfg.JournalTable, cfg.GcGraceSeconds,
	)
	if err := session.Query(messagesCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.JournalKeyspace, cfg.JournalTable, err)
	}

	metadataCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  persistence_id text,
  partition_nr   bigint,
  PRIMARY KEY (persistence_id)
)`,
		cfg.JournalKeyspace, cfg.MetadataTable,
	)
	if err := session.Query(metadataCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.JournalKeyspace, cfg.MetadataTable, err)
	}

	return nil
}

// createTagTables creates the tag_views and tag_scanning tables.
func createTagTables(session *gocql.Session, cfg *CassandraConfig) error {
	tagViewsCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  tag_name        text,
  timebucket      bigint,
  timestamp       timeuuid,
  persistence_id  text,
  sequence_nr     bigint,
  partition_nr    bigint,
  ser_id          int,
  ser_manifest    text,
  event_manifest  text,
  event           blob,
  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (timestamp ASC, persistence_id ASC, sequence_nr ASC)`,
		cfg.JournalKeyspace, cfg.TagTable,
	)
	if err := session.Query(tagViewsCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.JournalKeyspace, cfg.TagTable, err)
	}

	tagScanningCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  persistence_id text,
  tag            text,
  sequence_nr    bigint,
  PRIMARY KEY (persistence_id, tag)
)`,
		cfg.JournalKeyspace, cfg.ScanningTable,
	)
	if err := session.Query(tagScanningCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.JournalKeyspace, cfg.ScanningTable, err)
	}

	return nil
}

// createSnapshotTables creates the snapshots table in the snapshot keyspace.
func createSnapshotTables(session *gocql.Session, cfg *CassandraConfig) error {
	snapshotsCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  persistence_id    text,
  sequence_nr       bigint,
  timestamp         bigint,
  ser_id            int,
  ser_manifest      text,
  snapshot_data     blob,
  meta_ser_id       int,
  meta_ser_manifest text,
  meta              blob,
  PRIMARY KEY (persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (sequence_nr DESC)`,
		cfg.SnapshotKeyspace, cfg.SnapshotTable,
	)
	if err := session.Query(snapshotsCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.SnapshotKeyspace, cfg.SnapshotTable, err)
	}

	return nil
}

// createStateTables creates the durable_state table in the state keyspace.
func createStateTables(session *gocql.Session, cfg *CassandraConfig) error {
	durableStateCQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
  persistence_id text,
  revision       bigint,
  state_data     blob,
  state_manifest text,
  tag            text,
  PRIMARY KEY (persistence_id)
)`,
		cfg.StateKeyspace, cfg.StateTable,
	)
	if err := session.Query(durableStateCQL).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create table %s.%s: %w", cfg.StateKeyspace, cfg.StateTable, err)
	}

	return nil
}

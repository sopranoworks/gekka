/*
 * schema.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

// JournalDDL is the Cloud Spanner DDL statement that creates the journal table.
const JournalDDL = `CREATE TABLE IF NOT EXISTS journal (
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr    INT64       NOT NULL,
  payload        BYTES(MAX)  NOT NULL,
  manifest       STRING(MAX) NOT NULL,
  sender_path    STRING(MAX) NOT NULL,
  deleted        BOOL        NOT NULL,
  written_at     INT64       NOT NULL,
  tags           STRING(MAX) NOT NULL,
  commit_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (persistence_id, sequence_nr)`

// EventTagDDL is the Cloud Spanner DDL statement that creates the event_tag table.
const EventTagDDL = `CREATE TABLE IF NOT EXISTS event_tag (
  tag            STRING(MAX) NOT NULL,
  commit_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr    INT64       NOT NULL,
) PRIMARY KEY (tag, commit_timestamp, persistence_id, sequence_nr)`

// SnapshotsDDL is the Cloud Spanner DDL statement that creates the snapshots table.
const SnapshotsDDL = `CREATE TABLE IF NOT EXISTS snapshots (
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr    INT64       NOT NULL,
  snapshot_ts    INT64       NOT NULL,
  snapshot       BYTES(MAX)  NOT NULL,
  manifest       STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)`

// OffsetsDDL is the Cloud Spanner DDL statement that creates the offsets table.
const OffsetsDDL = `CREATE TABLE IF NOT EXISTS offsets (
  projection_name STRING(MAX) NOT NULL,
  offset_type     STRING(MAX) NOT NULL,
  offset_value    INT64       NOT NULL,
  updated_at      TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (projection_name)`

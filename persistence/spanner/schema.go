/*
 * schema.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

// JournalDDL is the Cloud Spanner DDL statement that creates the journal table.
//
// Columns:
//   - persistence_id : actor persistence identifier (part of PK)
//   - sequence_nr    : monotonically increasing event sequence (part of PK)
//   - payload        : JSON-encoded event payload bytes
//   - manifest       : Go type name used by PayloadCodec to reconstruct the value
//   - sender_path    : optional Artery actor path of the original sender
//   - deleted        : soft-delete flag (rows are physically deleted by AsyncDeleteMessagesTo)
//   - written_at     : wall-clock nanoseconds at write time (for diagnostics)
//   - tags           : comma-separated event tags
const JournalDDL = `CREATE TABLE IF NOT EXISTS journal (
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr    INT64       NOT NULL,
  payload        BYTES(MAX)  NOT NULL,
  manifest       STRING(MAX) NOT NULL,
  sender_path    STRING(MAX) NOT NULL,
  deleted        BOOL        NOT NULL,
  written_at     INT64       NOT NULL,
  tags           STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)`

// SnapshotsDDL is the Cloud Spanner DDL statement that creates the snapshots table.
//
// Columns:
//   - persistence_id : actor persistence identifier (part of PK)
//   - sequence_nr    : sequence number at snapshot time (part of PK)
//   - snapshot_ts    : wall-clock nanoseconds at snapshot time
//   - snapshot       : JSON-encoded snapshot bytes
//   - manifest       : Go type name used by PayloadCodec to reconstruct the value
const SnapshotsDDL = `CREATE TABLE IF NOT EXISTS snapshots (
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr    INT64       NOT NULL,
  snapshot_ts    INT64       NOT NULL,
  snapshot       BYTES(MAX)  NOT NULL,
  manifest       STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)`

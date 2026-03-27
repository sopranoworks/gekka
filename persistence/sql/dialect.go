/*
 * dialect.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

import "fmt"

// Dialect abstracts database-specific SQL syntax differences so that the
// journal and snapshot store implementations can target multiple databases
// without branching logic in the core operations.
type Dialect interface {
	// Placeholder returns the parameter placeholder for the n-th bind
	// parameter (1-indexed).  PostgreSQL uses "$1", "$2", …;
	// MySQL/SQLite use "?".
	Placeholder(n int) string

	// CreateJournalTableSQL returns a CREATE TABLE IF NOT EXISTS statement
	// for the events journal.  table is the unquoted table name.
	CreateJournalTableSQL(table string) string

	// CreateSnapshotTableSQL returns a CREATE TABLE IF NOT EXISTS statement
	// for the snapshot store.  table is the unquoted table name.
	CreateSnapshotTableSQL(table string) string

	// JournalInsertSQL returns the parameterised INSERT (or INSERT/UPSERT)
	// statement for a single journal row.
	// Bind order: persistence_id, sequence_nr, event_payload, event_manifest,
	//             sender_path, deleted, created_at, tags.
	JournalInsertSQL(table string) string

	// JournalHighestSeqNrSQL returns the SELECT statement that returns
	// MAX(sequence_nr) for a persistence ID.
	// Bind order: persistence_id, from_sequence_nr.
	JournalHighestSeqNrSQL(table string) string

	// JournalReplaySQL returns the SELECT statement for replaying events.
	// Bind order: persistence_id, from_seqnr, to_seqnr, limit.
	JournalReplaySQL(table string) string

	// JournalDeleteSQL returns the DELETE statement that removes all rows
	// for a persistence ID up to and including a sequence number.
	// Bind order: persistence_id, to_sequence_nr.
	JournalDeleteSQL(table string) string

	// JournalEventsByTagSQL returns the SELECT statement for events matching a tag.
	// Bind order: tag, offset_seq_nr, limit.
	JournalEventsByTagSQL(table string) string

	// JournalCurrentPersistenceIdsSQL returns the SELECT statement for current persistence IDs.
	JournalCurrentPersistenceIdsSQL(table string) string

	// JournalPersistenceIdsSQL returns the SELECT statement for continuous persistence IDs.
	// Bind order: offset_ordering, limit.
	JournalPersistenceIdsSQL(table string) string

	// SnapshotUpsertSQL returns the INSERT/UPSERT statement for a snapshot.
	// Bind order: persistence_id, sequence_nr, created_at,
	//             snapshot_payload, snapshot_manifest.
	SnapshotUpsertSQL(table string) string

	// SnapshotSelectSQL returns the SELECT statement for loading a snapshot
	// matching criteria, returning at most one row ordered by sequence_nr DESC.
	// Bind order: persistence_id, min_seqnr, max_seqnr,
	//             min_timestamp, max_timestamp.
	SnapshotSelectSQL(table string) string

	// SnapshotDeleteSQL returns the DELETE statement for a single snapshot.
	// Bind order: persistence_id, sequence_nr.
	SnapshotDeleteSQL(table string) string

	// SnapshotDeleteRangeSQL returns the DELETE statement for a range of
	// snapshots matching selection criteria.
	// Bind order: persistence_id, min_seqnr, max_seqnr,
	//             min_timestamp, max_timestamp.
	SnapshotDeleteRangeSQL(table string) string

	// CreateStateTableSQL returns a CREATE TABLE IF NOT EXISTS statement
	// for the durable state store.
	CreateStateTableSQL(table string) string

	// StateUpsertSQL returns the INSERT/UPSERT statement for durable state.
	// Bind order: persistence_id, revision, state_payload, state_manifest, tag, created_at.
	StateUpsertSQL(table string) string

	// StateSelectSQL returns the SELECT statement for loading the latest state.
	// Bind order: persistence_id.
	StateSelectSQL(table string) string

	// StateDeleteSQL returns the DELETE statement for a persistence ID.
	// Bind order: persistence_id.
	StateDeleteSQL(table string) string
}

// ── PostgresDialect ───────────────────────────────────────────────────────────

type PostgresDialect struct{}

func (PostgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (PostgresDialect) CreateJournalTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    ordering        BIGSERIAL    NOT NULL,
    persistence_id  VARCHAR(255) NOT NULL,
    sequence_nr     BIGINT       NOT NULL,
    event_payload   BYTEA        NOT NULL,
    event_manifest  VARCHAR(512) NOT NULL DEFAULT '',
    sender_path     VARCHAR(512) NOT NULL DEFAULT '',
    deleted         BOOLEAN      NOT NULL DEFAULT false,
    created_at      BIGINT       NOT NULL,
    tags            VARCHAR(512) NOT NULL DEFAULT '',
    PRIMARY KEY (persistence_id, sequence_nr)
);
CREATE INDEX IF NOT EXISTS ` + table + `_ordering ON ` + table + ` (ordering);
CREATE INDEX IF NOT EXISTS ` + table + `_tags ON ` + table + ` (tags);`
}

func (PostgresDialect) CreateSnapshotTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    persistence_id    VARCHAR(255) NOT NULL,
    sequence_nr       BIGINT       NOT NULL,
    created_at        BIGINT       NOT NULL,
    snapshot_payload  BYTEA        NOT NULL,
    snapshot_manifest VARCHAR(512) NOT NULL DEFAULT '',
    PRIMARY KEY (persistence_id, sequence_nr)
);`
}

func (PostgresDialect) JournalInsertSQL(table string) string {
	return `INSERT INTO ` + table + `
    (persistence_id, sequence_nr, event_payload, event_manifest, sender_path, deleted, created_at, tags)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (persistence_id, sequence_nr) DO NOTHING`
}

func (PostgresDialect) JournalHighestSeqNrSQL(table string) string {
	return `SELECT COALESCE(MAX(sequence_nr), 0)
FROM ` + table + `
WHERE persistence_id = $1 AND sequence_nr >= $2`
}

func (PostgresDialect) JournalReplaySQL(table string) string {
	return `SELECT persistence_id, sequence_nr, event_payload, event_manifest, sender_path, created_at, ordering
FROM ` + table + `
WHERE persistence_id = $1
  AND sequence_nr BETWEEN $2 AND $3
  AND deleted = false
ORDER BY sequence_nr ASC
LIMIT $4`
}

func (PostgresDialect) JournalEventsByTagSQL(table string) string {
	return `SELECT persistence_id, sequence_nr, event_payload, event_manifest, sender_path, created_at, ordering
FROM ` + table + `
WHERE tags LIKE '%' || $1 || '%'
  AND ordering > $2
  AND deleted = false
ORDER BY ordering ASC
LIMIT $3`
}

func (PostgresDialect) JournalCurrentPersistenceIdsSQL(table string) string {
	return `SELECT DISTINCT persistence_id FROM ` + table
}

func (PostgresDialect) JournalPersistenceIdsSQL(table string) string {
	return `SELECT persistence_id, MAX(ordering) as max_ord
FROM ` + table + `
GROUP BY persistence_id
HAVING MAX(ordering) > $1
ORDER BY max_ord ASC
LIMIT $2`
}

func (PostgresDialect) JournalDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = $1 AND sequence_nr <= $2`
}

func (PostgresDialect) SnapshotUpsertSQL(table string) string {
	return `INSERT INTO ` + table + `
    (persistence_id, sequence_nr, created_at, snapshot_payload, snapshot_manifest)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (persistence_id, sequence_nr) DO UPDATE
    SET created_at        = EXCLUDED.created_at,
        snapshot_payload  = EXCLUDED.snapshot_payload,
        snapshot_manifest = EXCLUDED.snapshot_manifest`
}

func (PostgresDialect) SnapshotSelectSQL(table string) string {
	return `SELECT sequence_nr, created_at, snapshot_payload, snapshot_manifest
FROM ` + table + `
WHERE persistence_id = $1
  AND sequence_nr BETWEEN $2 AND $3
  AND created_at  BETWEEN $4 AND $5
ORDER BY sequence_nr DESC
LIMIT 1`
}

func (PostgresDialect) SnapshotDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = $1 AND sequence_nr = $2`
}

func (PostgresDialect) SnapshotDeleteRangeSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = $1
  AND sequence_nr BETWEEN $2 AND $3
  AND created_at  BETWEEN $4 AND $5`
}

func (PostgresDialect) CreateStateTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    persistence_id  VARCHAR(255) NOT NULL,
    revision        BIGINT       NOT NULL,
    state_payload   BYTEA        NOT NULL,
    state_manifest  VARCHAR(512) NOT NULL DEFAULT '',
    tag             VARCHAR(512) NOT NULL DEFAULT '',
    created_at      BIGINT       NOT NULL,
    PRIMARY KEY (persistence_id)
);
CREATE INDEX IF NOT EXISTS ` + table + `_tag ON ` + table + ` (tag);`
}

func (PostgresDialect) StateUpsertSQL(table string) string {
	return `INSERT INTO ` + table + `
    (persistence_id, revision, state_payload, state_manifest, tag, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (persistence_id) DO UPDATE
    SET revision       = EXCLUDED.revision,
        state_payload  = EXCLUDED.state_payload,
        state_manifest = EXCLUDED.state_manifest,
        tag            = EXCLUDED.tag,
        created_at     = EXCLUDED.created_at`
}

func (PostgresDialect) StateSelectSQL(table string) string {
	return `SELECT revision, state_payload, state_manifest
FROM ` + table + `
WHERE persistence_id = $1`
}

func (PostgresDialect) StateDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = $1`
}

// ── MySQLDialect ──────────────────────────────────────────────────────────────

type MySQLDialect struct{}

func (MySQLDialect) Placeholder(_ int) string { return "?" }

func (MySQLDialect) CreateJournalTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    ordering        BIGINT       NOT NULL AUTO_INCREMENT,
    persistence_id  VARCHAR(255) NOT NULL,
    sequence_nr     BIGINT       NOT NULL,
    event_payload   BLOB         NOT NULL,
    event_manifest  VARCHAR(512) NOT NULL DEFAULT '',
    sender_path     VARCHAR(512) NOT NULL DEFAULT '',
    deleted         TINYINT(1)   NOT NULL DEFAULT 0,
    created_at      BIGINT       NOT NULL,
    tags            VARCHAR(512) NOT NULL DEFAULT '',
    PRIMARY KEY (persistence_id, sequence_nr),
    UNIQUE INDEX ` + table + `_ordering (ordering),
    INDEX ` + table + `_tags (tags)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

func (MySQLDialect) CreateSnapshotTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    persistence_id    VARCHAR(255) NOT NULL,
    sequence_nr       BIGINT       NOT NULL,
    created_at        BIGINT       NOT NULL,
    snapshot_payload  BLOB         NOT NULL,
    snapshot_manifest VARCHAR(512) NOT NULL DEFAULT '',
    PRIMARY KEY (persistence_id, sequence_nr)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

func (MySQLDialect) JournalInsertSQL(table string) string {
	return `INSERT IGNORE INTO ` + table + `
    (persistence_id, sequence_nr, event_payload, event_manifest, sender_path, deleted, created_at, tags)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
}

func (MySQLDialect) JournalHighestSeqNrSQL(table string) string {
	return `SELECT COALESCE(MAX(sequence_nr), 0)
FROM ` + table + `
WHERE persistence_id = ? AND sequence_nr >= ?`
}

func (MySQLDialect) JournalReplaySQL(table string) string {
	return `SELECT persistence_id, sequence_nr, event_payload, event_manifest, sender_path, created_at, ordering
FROM ` + table + `
WHERE persistence_id = ?
  AND sequence_nr BETWEEN ? AND ?
  AND deleted = 0
ORDER BY sequence_nr ASC
LIMIT ?`
}

func (MySQLDialect) JournalEventsByTagSQL(table string) string {
	return `SELECT persistence_id, sequence_nr, event_payload, event_manifest, sender_path, created_at, ordering
FROM ` + table + `
WHERE tags LIKE CONCAT('%', ?, '%')
  AND ordering > ?
  AND deleted = 0
ORDER BY ordering ASC
LIMIT ?`
}

func (MySQLDialect) JournalCurrentPersistenceIdsSQL(table string) string {
	return `SELECT DISTINCT persistence_id FROM ` + table
}

func (MySQLDialect) JournalPersistenceIdsSQL(table string) string {
	return `SELECT persistence_id, MAX(ordering) as max_ord
FROM ` + table + `
GROUP BY persistence_id
HAVING MAX(ordering) > ?
ORDER BY max_ord ASC
LIMIT ?`
}

func (MySQLDialect) JournalDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = ? AND sequence_nr <= ?`
}

func (MySQLDialect) SnapshotUpsertSQL(table string) string {
	return `INSERT INTO ` + table + `
    (persistence_id, sequence_nr, created_at, snapshot_payload, snapshot_manifest)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    created_at        = VALUES(created_at),
    snapshot_payload  = VALUES(snapshot_payload),
    snapshot_manifest = VALUES(snapshot_manifest)`
}

func (MySQLDialect) SnapshotSelectSQL(table string) string {
	return `SELECT sequence_nr, created_at, snapshot_payload, snapshot_manifest
FROM ` + table + `
WHERE persistence_id = ?
  AND sequence_nr BETWEEN ? AND ?
  AND created_at  BETWEEN ? AND ?
ORDER BY sequence_nr DESC
LIMIT 1`
}

func (MySQLDialect) SnapshotDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = ? AND sequence_nr = ?`
}

func (MySQLDialect) SnapshotDeleteRangeSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = ?
  AND sequence_nr BETWEEN ? AND ?
  AND created_at  BETWEEN ? AND ?`
}

func (MySQLDialect) CreateStateTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
    persistence_id  VARCHAR(255) NOT NULL,
    revision        BIGINT       NOT NULL,
    state_payload   BLOB         NOT NULL,
    state_manifest  VARCHAR(512) NOT NULL DEFAULT '',
    tag             VARCHAR(512) NOT NULL DEFAULT '',
    created_at      BIGINT       NOT NULL,
    PRIMARY KEY (persistence_id),
    INDEX ` + table + `_tag (tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

func (MySQLDialect) StateUpsertSQL(table string) string {
	return `INSERT INTO ` + table + `
    (persistence_id, revision, state_payload, state_manifest, tag, created_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    revision       = VALUES(revision),
    state_payload  = VALUES(state_payload),
    state_manifest = VALUES(state_manifest),
    tag            = VALUES(tag),
    created_at     = VALUES(created_at)`
}

func (MySQLDialect) StateSelectSQL(table string) string {
	return `SELECT revision, state_payload, state_manifest
FROM ` + table + `
WHERE persistence_id = ?`
}

func (MySQLDialect) StateDeleteSQL(table string) string {
	return `DELETE FROM ` + table + `
WHERE persistence_id = ?`
}

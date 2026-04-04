/*
 * persistence/projection/offset_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package projection

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sopranoworks/gekka/persistence/query"
)

// OffsetStore is responsible for persisting and retrieving projection offsets.
type OffsetStore interface {
	// ReadOffset retrieves the last saved offset for a given projection.
	ReadOffset(ctx context.Context, projectionName string) (query.Offset, error)

	// SaveOffset persists the offset for a given projection.
	SaveOffset(ctx context.Context, projectionName string, offset query.Offset) error
}

// TransactionalOffsetStore extends OffsetStore with transactional offset saving.
// Implementations must persist the offset as part of an existing *sql.Tx so that
// the handler's side-effects and the offset advance are committed atomically.
type TransactionalOffsetStore interface {
	OffsetStore
	// SaveOffsetTx persists the offset within the given transaction.
	// The caller is responsible for committing or rolling back the transaction.
	SaveOffsetTx(ctx context.Context, tx *sql.Tx, projectionName string, offset query.Offset) error
}

// sqlOffsetStore implements OffsetStore using a SQL database.
type sqlOffsetStore struct {
	db    *sql.DB
	table string
}

// NewSQLOffsetStore creates a new OffsetStore backed by a SQL database.
// The returned value also implements TransactionalOffsetStore.
func NewSQLOffsetStore(db *sql.DB, table string) TransactionalOffsetStore {
	return &sqlOffsetStore{
		db:    db,
		table: table,
	}
}

// CreateTable creates the offset store table if it doesn't exist.
func (s *sqlOffsetStore) CreateTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		projection_name VARCHAR(255) NOT NULL,
		offset_type     VARCHAR(50)  NOT NULL,
		offset_value    BIGINT       NOT NULL,
		updated_at      BIGINT       NOT NULL,
		PRIMARY KEY (projection_name)
	)`, s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

func (s *sqlOffsetStore) ReadOffset(ctx context.Context, projectionName string) (query.Offset, error) {
	var offsetType string
	var offsetValue int64

	queryStr := fmt.Sprintf("SELECT offset_type, offset_value FROM %s WHERE projection_name = ?", s.table)
	err := s.db.QueryRowContext(ctx, queryStr, projectionName).Scan(&offsetType, &offsetValue)
	if err == sql.ErrNoRows {
		return query.NoOffset{}, nil
	}
	if err != nil {
		return nil, err
	}

	switch offsetType {
	case "SequenceOffset":
		return query.SequenceOffset(offsetValue), nil
	case "TimeOffset":
		return query.TimeOffset(offsetValue), nil
	default:
		return nil, fmt.Errorf("unknown offset type: %s", offsetType)
	}
}

func (s *sqlOffsetStore) SaveOffset(ctx context.Context, projectionName string, offset query.Offset) error {
	offsetType, offsetValue, err := offsetToSQL(offset)
	if err != nil {
		return err
	}
	if offsetType == "" {
		return nil // NoOffset — nothing to save
	}
	now := time.Now().UnixNano()
	queryStr := fmt.Sprintf(`INSERT INTO %s (projection_name, offset_type, offset_value, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE offset_type = VALUES(offset_type), offset_value = VALUES(offset_value), updated_at = VALUES(updated_at)`,
		s.table)
	_, err = s.db.ExecContext(ctx, queryStr, projectionName, offsetType, offsetValue, now)
	return err
}

// SaveOffsetTx persists the offset within the provided transaction, enabling
// exactly-once delivery when the handler's side-effects use the same transaction.
func (s *sqlOffsetStore) SaveOffsetTx(ctx context.Context, tx *sql.Tx, projectionName string, offset query.Offset) error {
	offsetType, offsetValue, err := offsetToSQL(offset)
	if err != nil {
		return err
	}
	if offsetType == "" {
		return nil // NoOffset — nothing to save
	}
	now := time.Now().UnixNano()
	queryStr := fmt.Sprintf(`INSERT INTO %s (projection_name, offset_type, offset_value, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE offset_type = VALUES(offset_type), offset_value = VALUES(offset_value), updated_at = VALUES(updated_at)`,
		s.table)
	_, err = tx.ExecContext(ctx, queryStr, projectionName, offsetType, offsetValue, now)
	return err
}

// offsetToSQL converts a query.Offset to a (type, value) pair for SQL storage.
// Returns ("", 0, nil) for NoOffset (nothing to persist).
func offsetToSQL(offset query.Offset) (string, int64, error) {
	switch o := offset.(type) {
	case query.SequenceOffset:
		return "SequenceOffset", int64(o), nil
	case query.TimeOffset:
		return "TimeOffset", int64(o), nil
	case query.NoOffset:
		return "", 0, nil
	default:
		return "", 0, fmt.Errorf("unsupported offset type: %T", offset)
	}
}

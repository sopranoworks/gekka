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

// sqlOffsetStore implements OffsetStore using a SQL database.
type sqlOffsetStore struct {
	db    *sql.DB
	table string
}

// NewSQLOffsetStore creates a new OffsetStore implementation for SQL.
func NewSQLOffsetStore(db *sql.DB, table string) OffsetStore {
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
	var offsetType string
	var offsetValue int64

	switch o := offset.(type) {
	case query.SequenceOffset:
		offsetType = "SequenceOffset"
		offsetValue = int64(o)
	case query.TimeOffset:
		offsetType = "TimeOffset"
		offsetValue = int64(o)
	case query.NoOffset:
		return nil // nothing to save
	default:
		return fmt.Errorf("unsupported offset type: %T", offset)
	}

	now := time.Now().UnixNano()
	queryStr := fmt.Sprintf(`INSERT INTO %s (projection_name, offset_type, offset_value, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE offset_type = VALUES(offset_type), offset_value = VALUES(offset_value), updated_at = VALUES(updated_at)`,
		s.table)

	// Implementation note: This UPSERT syntax is MySQL specific.
	// In a real implementation we would use the Dialect to get the correct SQL.

	_, err := s.db.ExecContext(ctx, queryStr, projectionName, offsetType, offsetValue, now)
	return err
}

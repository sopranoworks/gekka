/*
 * state_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SQLDurableStateStore implements persistence.DurableStateStore using a SQL database.
type SQLDurableStateStore struct {
	db      *sql.DB
	dialect Dialect
	codec   PayloadCodec
	config  Config
}

// NewSQLDurableStateStore creates a new SQLDurableStateStore.
func NewSQLDurableStateStore(db *sql.DB, dialect Dialect, codec PayloadCodec, config Config) *SQLDurableStateStore {
	return &SQLDurableStateStore{
		db:      db,
		dialect: dialect,
		codec:   codec,
		config:  config,
	}
}

// Get implements persistence.DurableStateStore.
func (s *SQLDurableStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	row := s.db.QueryRowContext(ctx, s.dialect.StateSelectSQL(s.config.stateTable()), persistenceID)

	var revision uint64
	var payload []byte
	var manifest string

	if err := row.Scan(&revision, &payload, &manifest); err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("sqlstore: get state %q: %w", persistenceID, err)
	}

	state, err := s.codec.Decode(manifest, payload)
	if err != nil {
		return nil, 0, fmt.Errorf("sqlstore: decode state %q: %w", persistenceID, err)
	}

	return state, revision, nil
}

// Upsert implements persistence.DurableStateStore.
func (s *SQLDurableStateStore) Upsert(ctx context.Context, persistenceID string, revision uint64, state any, tag string) error {
	manifest, payload, err := s.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("sqlstore: encode state %q: %w", persistenceID, err)
	}

	createdAt := time.Now().UnixMilli()

	_, err = s.db.ExecContext(ctx, s.dialect.StateUpsertSQL(s.config.stateTable()),
		persistenceID, revision, payload, manifest, tag, createdAt)
	if err != nil {
		return fmt.Errorf("sqlstore: upsert state %q: %w", persistenceID, err)
	}

	return nil
}

// Delete implements persistence.DurableStateStore.
func (s *SQLDurableStateStore) Delete(ctx context.Context, persistenceID string) error {
	_, err := s.db.ExecContext(ctx, s.dialect.StateDeleteSQL(s.config.stateTable()), persistenceID)
	if err != nil {
		return fmt.Errorf("sqlstore: delete state %q: %w", persistenceID, err)
	}
	return nil
}

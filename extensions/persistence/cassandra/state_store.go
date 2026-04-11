/*
 * state_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraDurableStateStore implements persistence.DurableStateStore backed
// by Apache Cassandra using the Pekko-compatible durable_state schema.
type CassandraDurableStateStore struct {
	session  *gocql.Session
	keyspace string
	table    string
	codec    PayloadCodec
}

// NewCassandraDurableStateStore constructs a CassandraDurableStateStore from a
// live session and config.
func NewCassandraDurableStateStore(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraDurableStateStore {
	return &CassandraDurableStateStore{
		session:  session,
		keyspace: cfg.StateKeyspace,
		table:    cfg.StateTable,
		codec:    codec,
	}
}

// Get retrieves the current state and its revision for the given persistence ID.
// Returns (nil, 0, nil) when no state has been stored yet.
func (s *CassandraDurableStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	q := fmt.Sprintf(
		`SELECT revision, state_data, state_manifest, tag FROM %s.%s WHERE persistence_id = ?`,
		s.keyspace, s.table,
	)
	var (
		revision int64
		data     []byte
		manifest string
		tag      string
	)
	err := s.session.Query(q, persistenceID).WithContext(ctx).Scan(&revision, &data, &manifest, &tag)
	if err != nil {
		if err.Error() == "not found" {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("cassandrastatestore: get %q: %w", persistenceID, err)
	}

	state, decErr := s.codec.Decode(manifest, data)
	if decErr != nil {
		return nil, 0, fmt.Errorf("cassandrastatestore: decode %q: %w", persistenceID, decErr)
	}
	return state, uint64(revision), nil //nolint:gosec
}

// Upsert inserts or replaces the state for persistenceID at the given revision.
func (s *CassandraDurableStateStore) Upsert(ctx context.Context, persistenceID string, seqNr uint64, state any, tag string) error {
	manifest, data, err := s.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("cassandrastatestore: encode %q: %w", persistenceID, err)
	}

	q := fmt.Sprintf(
		`INSERT INTO %s.%s (persistence_id, revision, state_data, state_manifest, tag) VALUES (?, ?, ?, ?, ?)`,
		s.keyspace, s.table,
	)
	if err := s.session.Query(q,
		persistenceID,
		int64(seqNr), //nolint:gosec
		data,
		manifest,
		tag,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrastatestore: upsert %q: %w", persistenceID, err)
	}
	return nil
}

// Delete removes the state for the given persistence ID.
func (s *CassandraDurableStateStore) Delete(ctx context.Context, persistenceID string) error {
	q := fmt.Sprintf(
		`DELETE FROM %s.%s WHERE persistence_id = ?`,
		s.keyspace, s.table,
	)
	if err := s.session.Query(q, persistenceID).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrastatestore: delete %q: %w", persistenceID, err)
	}
	return nil
}

// Compile-time check that CassandraDurableStateStore satisfies persistence.DurableStateStore.
var _ persistence.DurableStateStore = (*CassandraDurableStateStore)(nil)

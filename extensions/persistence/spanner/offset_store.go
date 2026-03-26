/*
 * offset_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/sopranoworks/gekka/persistence/query"
	"google.golang.org/api/iterator"
)

// SpannerOffsetStore implements projection.OffsetStore using Cloud Spanner.
type SpannerOffsetStore struct {
	client *spanner.Client
	table  string
}

// NewSpannerOffsetStore creates a new SpannerOffsetStore.
func NewSpannerOffsetStore(client *spanner.Client, table string) *SpannerOffsetStore {
	if table == "" {
		table = "offsets"
	}
	return &SpannerOffsetStore{
		client: client,
		table:  table,
	}
}

func (s *SpannerOffsetStore) ReadOffset(ctx context.Context, projectionName string) (query.Offset, error) {
	stmt := spanner.Statement{
		SQL:    fmt.Sprintf("SELECT offset_type, offset_value FROM %s WHERE projection_name = @name", s.table),
		Params: map[string]any{"name": projectionName},
	}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return query.NoOffset{}, nil
	}
	if err != nil {
		return nil, err
	}

	var offsetType string
	var offsetValue int64
	if err := row.Columns(&offsetType, &offsetValue); err != nil {
		return nil, err
	}

	switch offsetType {
	case "SequenceOffset":
		return query.SequenceOffset(offsetValue), nil
	case "TimeOffset":
		return query.TimeOffset(offsetValue), nil
	default:
		return nil, fmt.Errorf("spanneroffsetstore: unknown offset type: %s", offsetType)
	}
}

func (s *SpannerOffsetStore) SaveOffset(ctx context.Context, projectionName string, offset query.Offset) error {
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
		return nil
	default:
		return fmt.Errorf("spanneroffsetstore: unsupported offset type: %T", offset)
	}

	mut := spanner.InsertOrUpdate(s.table,
		[]string{"projection_name", "offset_type", "offset_value", "updated_at"},
		[]any{projectionName, offsetType, offsetValue, spanner.CommitTimestamp},
	)

	_, err := s.client.Apply(ctx, []*spanner.Mutation{mut})
	return err
}

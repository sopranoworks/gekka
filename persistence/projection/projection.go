/*
 * persistence/projection/projection.go
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
	"log"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"github.com/sopranoworks/gekka/telemetry"
)

// Projection represents a component that processes an event stream and maintains
// a derived state or side effect (e.g., a read model).
type Projection interface {
	// Run starts the projection processing loop.
	Run(ctx context.Context) error
}

// SourceProvider provides the event source for a projection starting from a given offset.
type SourceProvider interface {
	Source(ctx context.Context, offset query.Offset) (stream.Source[query.EventEnvelope, stream.NotUsed], error)
}

// Handler is a functional interface for processing a single event envelope.
type Handler func(envelope query.EventEnvelope) error

const projectionTracingScope = "github.com/sopranoworks/gekka/persistence/projection"

// projectionRunner is the default implementation of Projection.
type projectionRunner struct {
	name           string
	sourceProvider SourceProvider
	offsetStore    OffsetStore
	handler        Handler
}

// NewProjection creates a new Projection instance.
func NewProjection(name string, sp SourceProvider, os OffsetStore, h Handler) Projection {
	return &projectionRunner{
		name:           name,
		sourceProvider: sp,
		offsetStore:    os,
		handler:        h,
	}
}

// ReliableProducerHandler returns a Handler that forwards envelopes to a ProducerController.
func ReliableProducerHandler(producerRef actor.Ref, serializerID int32) Handler {
	return func(envelope query.EventEnvelope) error {
		// Forward message to ProducerController for reliable delivery.
		// Note: Application-level serialization is assumed to be handled by the caller or a codec.
		var payload []byte
		if b, ok := envelope.Event.([]byte); ok {
			payload = b
		} else {
			// fallback to a simple representation
			payload = []byte(fmt.Sprintf("%v", envelope.Event))
		}

		producerRef.Tell(SendMessageWrapper{
			Payload:      payload,
			SerializerID: serializerID,
		})
		return nil
	}
}

// SendMessageWrapper matches the structure expected by ProducerController.
type SendMessageWrapper struct {
	Payload      []byte
	SerializerID int32
	Manifest     string
}

// Run executes the projection loop.
func (r *projectionRunner) Run(ctx context.Context) error {
	// 1. Load initial offset.
	offset, err := r.offsetStore.ReadOffset(ctx, r.name)
	if err != nil {
		return fmt.Errorf("projection %q: failed to read offset: %w", r.name, err)
	}
	if offset == nil {
		offset = query.NoOffset{}
	}

	// 2. Fetch source.
	src, err := r.sourceProvider.Source(ctx, offset)
	if err != nil {
		return fmt.Errorf("projection %q: failed to create source: %w", r.name, err)
	}

	log.Printf("Projection %q: starting from offset %v", r.name, offset)

	// 3. Process stream.
	return r.runManual(ctx, src)
}

// ── Exactly-once projection ───────────────────────────────────────────────────

// TransactionalHandler processes a single event envelope within an existing
// *sql.Tx.  Both the handler's side-effects and the subsequent offset save
// are committed in the same transaction, achieving exactly-once delivery when
// the handler writes to the same database as the offset store.
type TransactionalHandler func(ctx context.Context, tx *sql.Tx, envelope query.EventEnvelope) error

// exactlyOnceProjectionRunner wraps each event in a single sql.Tx that
// atomically commits the handler's side-effect and the offset advance.
type exactlyOnceProjectionRunner struct {
	name           string
	sourceProvider SourceProvider
	offsetStore    TransactionalOffsetStore
	handler        TransactionalHandler
	db             *sql.DB
}

// NewExactlyOnceProjection creates a Projection that delivers each event
// exactly once by wrapping the handler invocation and offset save in a single
// database transaction.
//
//   - db          — the database used for both the handler and the offset store
//   - sp          — provides the event source starting from a given offset
//   - os          — a TransactionalOffsetStore backed by the same db
//   - h           — handler that performs its side-effect within the provided tx
func NewExactlyOnceProjection(
	name string,
	sp SourceProvider,
	os TransactionalOffsetStore,
	db *sql.DB,
	h TransactionalHandler,
) Projection {
	return &exactlyOnceProjectionRunner{
		name:           name,
		sourceProvider: sp,
		offsetStore:    os,
		handler:        h,
		db:             db,
	}
}

// Run executes the exactly-once projection loop.
func (r *exactlyOnceProjectionRunner) Run(ctx context.Context) error {
	offset, err := r.offsetStore.ReadOffset(ctx, r.name)
	if err != nil {
		return fmt.Errorf("projection %q: failed to read offset: %w", r.name, err)
	}
	if offset == nil {
		offset = query.NoOffset{}
	}

	src, err := r.sourceProvider.Source(ctx, offset)
	if err != nil {
		return fmt.Errorf("projection %q: failed to create source: %w", r.name, err)
	}

	log.Printf("ExactlyOnceProjection %q: starting from offset %v", r.name, offset)

	return r.runExactlyOnce(ctx, src)
}

// runExactlyOnce processes each event in its own sql.Tx.
// The transaction atomically commits:
//  1. The handler's write(s) to the database.
//  2. The updated projection offset.
//
// If either step fails the transaction is rolled back and the error is
// returned, leaving the offset unchanged so the event is retried.
func (r *exactlyOnceProjectionRunner) runExactlyOnce(ctx context.Context, src stream.Source[query.EventEnvelope, stream.NotUsed]) error {
	m := stream.SyncMaterializer{}

	tracer := telemetry.GetTracer(projectionTracingScope)

	_, err := stream.RunWith(src, stream.ForeachErr(func(env query.EventEnvelope) error {
		eventCtx := ctx
		if len(env.TraceContext) > 0 {
			eventCtx = tracer.Extract(ctx, env.TraceContext)
		}
		_, span := tracer.Start(eventCtx, "ExactlyOnceProjection.Handle")
		defer span.End()

		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("projection %q: begin tx: %w", r.name, err)
		}

		if err := r.handler(ctx, tx, env); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := r.offsetStore.SaveOffsetTx(ctx, tx, r.name, env.Offset); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("projection %q: save offset in tx: %w", r.name, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("projection %q: commit tx: %w", r.name, err)
		}
		return nil
	}), m)

	return err
}

// ─────────────────────────────────────────────────────────────────────────────

func (r *projectionRunner) runManual(ctx context.Context, src stream.Source[query.EventEnvelope, stream.NotUsed]) error {
	// Use SyncMaterializer for synchronous execution.
	m := stream.SyncMaterializer{}

	tracer := telemetry.GetTracer(projectionTracingScope)

	_, err := stream.RunWith(src, stream.ForeachErr(func(env query.EventEnvelope) error {
		// Extract the write-side trace context stored in the event envelope and
		// start a child span so the projection handling is linked to the original
		// command's trace.
		eventCtx := ctx
		if len(env.TraceContext) > 0 {
			eventCtx = tracer.Extract(ctx, env.TraceContext)
		}
		_, span := tracer.Start(eventCtx, "Projection.Handle")

		err := r.handler(env)
		span.End()
		if err != nil {
			return err
		}
		// Save offset after successful processing.
		if err := r.offsetStore.SaveOffset(ctx, r.name, env.Offset); err != nil {
			return err
		}
		return nil
	}), m)

	return err
}

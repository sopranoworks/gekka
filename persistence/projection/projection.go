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

/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracingInstrumentationName is the OTel instrumentation scope for the persistence layer.
const tracingInstrumentationName = "github.com/sopranoworks/gekka/persistence"

// PersistentRepr is an envelope for a persistent event.
//
// TraceContext carries the W3C TraceContext headers that were active when the
// command that caused this event was processed.  The TracingJournal uses this
// to create a "Journal.Write" child span linked to the originating trace, and
// the read side copies it into EventEnvelope so projections can continue the
// same trace on the read path.
type PersistentRepr struct {
	PersistenceID string
	SequenceNr    uint64
	Payload       any
	Deleted       bool
	SenderPath    string
	Tags          []string
	TraceContext  map[string]string // W3C TraceContext headers; nil if tracing not active
}

// Journal is the interface for storing and replaying events.
type Journal interface {
	// ReplayMessages replays messages from fromSequenceNr to toSequenceNr (inclusive)
	// for the given persistenceId. The callback is invoked for each message.
	ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error

	// ReadHighestSequenceNr returns the highest sequence number stored for the given persistenceId.
	ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error)

	// AsyncWriteMessages asynchronously stores a batch of messages.
	AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error

	// AsyncDeleteMessagesTo deletes all messages up to (and including) the given sequenceNr.
	AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error
}

// ── TracingJournal ─────────────────────────────────────────────────────────────

// TracingJournal wraps a Journal implementation and creates OpenTelemetry spans
// for each write and read (replay) operation.
//
//   - AsyncWriteMessages: for each message batch, extracts the TraceContext from
//     the first non-nil PersistentRepr.TraceContext and starts a child span named
//     "Journal.Write".  The span is ended when the inner write returns.
//
//   - ReplayMessages: starts a span named "Journal.Read" linked to ctx, delegates
//     to the inner journal, and ends the span after all events are delivered.
//
// All other Journal methods are delegated without modification.
type TracingJournal struct {
	inner Journal
}

// NewTracingJournal wraps inner with span-emitting instrumentation.
func NewTracingJournal(inner Journal) *TracingJournal {
	return &TracingJournal{inner: inner}
}

func (t *TracingJournal) tracer() trace.Tracer {
	return otel.Tracer(tracingInstrumentationName)
}

func (t *TracingJournal) extractCtx(tc map[string]string) context.Context {
	if len(tc) == 0 {
		return context.Background()
	}
	return otel.GetTextMapPropagator().Extract(
		context.Background(),
		propagation.MapCarrier(tc),
	)
}

// AsyncWriteMessages starts a "Journal.Write" child span linked to the first
// non-nil TraceContext in the batch, then delegates to the wrapped journal.
func (t *TracingJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	spanCtx := ctx
	for _, m := range messages {
		if len(m.TraceContext) > 0 {
			spanCtx = t.extractCtx(m.TraceContext)
			break
		}
	}
	_, span := t.tracer().Start(spanCtx, "Journal.Write")
	defer span.End()
	return t.inner.AsyncWriteMessages(ctx, messages)
}

// ReplayMessages starts a "Journal.Read" span then delegates to the wrapped journal.
func (t *TracingJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	_, span := t.tracer().Start(ctx, "Journal.Read")
	defer span.End()
	return t.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
}

// ReadHighestSequenceNr delegates without instrumentation.
func (t *TracingJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return t.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

// AsyncDeleteMessagesTo delegates without instrumentation.
func (t *TracingJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return t.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

// Ensure TracingJournal satisfies Journal at compile time.
var _ Journal = (*TracingJournal)(nil)

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
	"log"

	"github.com/sopranoworks/gekka/telemetry"
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
	Manifest      string // Manifest for the payload, used by EventAdapters for schema evolution
	Deleted       bool
	SenderPath    string
	Tags          []string
	TraceContext  map[string]string // W3C TraceContext headers; nil if tracing not active

	// WriterUuid is the per-writer fingerprint Pekko stores alongside
	// each event so that the replay filter (see replay_filter.go) can
	// detect duplicated sequence numbers from a previous incarnation
	// of the same persistent actor. Empty for events written by code
	// that pre-dates the filter; the filter treats empty UUIDs as
	// "writer unknown" and bypasses cross-writer conflict detection
	// for them. Round-2 session 38 — F10 plugin-fallback.
	WriterUuid string
}

// EventAdapter is used for schema evolution. It allows transforming events
// before they are written to the journal and after they are read.
type EventAdapter interface {
	// ToJournal transforms the given event to a form suitable for the journal.
	// It can return multiple events (e.g. for event splitting) or an empty slice.
	ToJournal(event any) []any

	// FromJournal transforms the given event from its journal representation.
	// It returns the transformed event and its manifest.
	FromJournal(event any, manifest string) (any, error)

	// Manifest returns the manifest for the given event.
	Manifest(event any) string
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

// ── AdaptedJournal ─────────────────────────────────────────────────────────────

// AdaptedJournal wraps a Journal and applies EventAdapters to events.
type AdaptedJournal struct {
	inner    Journal
	adapters map[string]EventAdapter // persistenceId prefix or exact match -> adapter
}

func NewAdaptedJournal(inner Journal) *AdaptedJournal {
	return &AdaptedJournal{
		inner:    inner,
		adapters: make(map[string]EventAdapter),
	}
}

func (a *AdaptedJournal) AddAdapter(prefix string, adapter EventAdapter) {
	a.adapters[prefix] = adapter
}

func (a *AdaptedJournal) getAdapter(persistenceId string) EventAdapter {
	// Simplified: find the longest matching prefix
	// In a real implementation this might be more sophisticated
	return a.adapters[persistenceId]
}

func (a *AdaptedJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	var adapted []PersistentRepr
	for _, m := range messages {
		adapter := a.getAdapter(m.PersistenceID)
		if adapter == nil {
			adapted = append(adapted, m)
			continue
		}

		toWrite := adapter.ToJournal(m.Payload)
		for _, payload := range toWrite {
			cp := m
			cp.Payload = payload
			cp.Manifest = adapter.Manifest(payload)
			adapted = append(adapted, cp)
		}
	}
	return a.inner.AsyncWriteMessages(ctx, adapted)
}

func (a *AdaptedJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	adapter := a.getAdapter(persistenceId)
	if adapter == nil {
		return a.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
	}

	return a.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, func(m PersistentRepr) {
		transformed, err := adapter.FromJournal(m.Payload, m.Manifest)
		if err != nil {
			// In case of error, we still pass the original or log it?
			// Pekko usually fails the recovery.
			log.Printf("AdaptedJournal: fromJournal error for %s: %v", persistenceId, err)
			return
		}
		m.Payload = transformed
		callback(m)
	})
}

func (a *AdaptedJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return a.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

func (a *AdaptedJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return a.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

var _ Journal = (*AdaptedJournal)(nil)

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

func (t *TracingJournal) tracer() telemetry.Tracer {
	return telemetry.GetTracer(tracingInstrumentationName)
}

func (t *TracingJournal) extractCtx(tc map[string]string) context.Context {
	if len(tc) == 0 {
		return context.Background()
	}
	return telemetry.GetTracer(tracingInstrumentationName).Extract(context.Background(), tc)
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

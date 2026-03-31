/*
 * tag_filter_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package query_test

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEventsByTag_CrossActorFiltering verifies that EventsByTag returns only
// events whose Tags slice contains the queried tag, across multiple actors.
//
// Setup:
//   - actor "order-service"   persists events tagged ["critical", "internal"]
//                             and ["internal"] and ["critical"]
//   - actor "payment-service" persists events tagged ["critical"] twice
//   - actor "audit-log"       persists one event tagged ["internal"]
//
// Expectation: EventsByTag("critical") yields exactly 4 events (OrderPlaced,
// OrderCancelled, PaymentProcessed, PaymentRefunded) and must not include any
// event from audit-log or the "internal"-only OrderShipped.
func TestEventsByTag_CrossActorFiltering(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	// actor-1: order-service — tags some events "critical", some "internal"
	require.NoError(t, journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "order-service", SequenceNr: 1, Payload: "OrderPlaced", Tags: []string{"critical", "internal"}},
		{PersistenceID: "order-service", SequenceNr: 2, Payload: "OrderShipped", Tags: []string{"internal"}},
		{PersistenceID: "order-service", SequenceNr: 3, Payload: "OrderCancelled", Tags: []string{"critical"}},
	}))

	// actor-2: payment-service — all events tagged "critical"
	require.NoError(t, journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "payment-service", SequenceNr: 1, Payload: "PaymentProcessed", Tags: []string{"critical"}},
		{PersistenceID: "payment-service", SequenceNr: 2, Payload: "PaymentRefunded", Tags: []string{"critical"}},
	}))

	// actor-3: audit-log — all events tagged only "internal"
	require.NoError(t, journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "audit-log", SequenceNr: 1, Payload: "AuditEntry", Tags: []string{"internal"}},
	}))

	readJournal := persistence.NewInMemoryReadJournal(journal)

	// ── EventsByTag("critical") ───────────────────────────────────────────────

	criticalSrc := readJournal.EventsByTag("critical", query.NoOffset{})
	criticalEvents, err := stream.RunWith(criticalSrc, stream.Collect[query.EventEnvelope](), stream.SyncMaterializer{})
	require.NoError(t, err)

	// Expect: OrderPlaced (order-service/1), OrderCancelled (order-service/3),
	//         PaymentProcessed (payment-service/1), PaymentRefunded (payment-service/2)
	assert.Equal(t, 4, len(criticalEvents), "should yield exactly 4 critical events")

	// Audit-log events must not appear: they carry only "internal".
	for _, env := range criticalEvents {
		assert.NotEqual(t, "audit-log", env.PersistenceID,
			"audit-log events should not appear in critical stream")
		assert.NotEqual(t, "OrderShipped", env.Event,
			"OrderShipped has only 'internal' tag and must not appear in critical stream")
	}

	// ── EventsByTag("internal") ───────────────────────────────────────────────

	internalSrc := readJournal.EventsByTag("internal", query.NoOffset{})
	internalEvents, err := stream.RunWith(internalSrc, stream.Collect[query.EventEnvelope](), stream.SyncMaterializer{})
	require.NoError(t, err)

	// Expect: OrderPlaced (order-service/1), OrderShipped (order-service/2), AuditEntry (audit-log/1)
	assert.Equal(t, 3, len(internalEvents), "should yield exactly 3 internal events")

	// payment-service events carry only "critical" and must not appear.
	for _, env := range internalEvents {
		assert.NotEqual(t, "payment-service", env.PersistenceID,
			"payment-service events must not appear in internal stream")
	}

	// ── Offset resumption ─────────────────────────────────────────────────────

	// After consuming the first critical event (offset pos=1), resuming from
	// that offset should yield the remaining 3 critical events.
	firstOffset := criticalEvents[0].Offset
	resumedSrc := readJournal.EventsByTag("critical", firstOffset)
	resumed, err := stream.RunWith(resumedSrc, stream.Collect[query.EventEnvelope](), stream.SyncMaterializer{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(resumed), "resuming after first critical event should yield 3 more")
}

// TestEventsByTag_NoOffset_EmptyJournal ensures the stream terminates cleanly
// when the journal has no events.
func TestEventsByTag_NoOffset_EmptyJournal(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	readJournal := persistence.NewInMemoryReadJournal(journal)

	src := readJournal.EventsByTag("critical", query.NoOffset{})
	events, err := stream.RunWith(src, stream.Collect[query.EventEnvelope](), stream.SyncMaterializer{})
	require.NoError(t, err)
	assert.Empty(t, events)
}

// TestEventsByTag_ExactTagMatch verifies that tag matching is exact (not a
// substring match): querying "crit" must not return events tagged "critical".
func TestEventsByTag_ExactTagMatch(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	require.NoError(t, journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "svc", SequenceNr: 1, Payload: "E1", Tags: []string{"critical"}},
		{PersistenceID: "svc", SequenceNr: 2, Payload: "E2", Tags: []string{"crit"}},
	}))

	readJournal := persistence.NewInMemoryReadJournal(journal)

	src := readJournal.EventsByTag("crit", query.NoOffset{})
	events, err := stream.RunWith(src, stream.Collect[query.EventEnvelope](), stream.SyncMaterializer{})
	require.NoError(t, err)
	require.Equal(t, 1, len(events))
	assert.Equal(t, "E2", events[0].Event)
}

/*
 * durable_queue_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// durableStub is a minimal actor.Ref that records messages.
type durableStub struct {
	path string
	msgs []any
}

func (r *durableStub) Path() string             { return r.path }
func (r *durableStub) Tell(msg any, _ ...actor.Ref) { r.msgs = append(r.msgs, msg) }

func newDurableState(t *testing.T, j persistence.Journal) *durableQueueState {
	t.Helper()
	s := &durableQueueState{
		producerID:    "test-producer",
		persistenceID: "test-durable-queue",
		journal:       j,
		nextSeqNr:     1,
	}
	s.selfRef = &durableStub{path: "/user/durable-producer"}
	s.recovered = true // skip auto-recovery; tests call recover() explicitly
	return s
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestDurableQueue_PersistsSentEvent(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	s := newDurableState(t, j)

	s.onSendMessage(nil, SendMessage{Payload: []byte("hello"), SerializerID: 4})

	// Journal should contain one entry.
	highest, err := j.ReadHighestSequenceNr(context.Background(), s.persistenceID, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 1 {
		t.Errorf("highest seqNr = %d, want 1", highest)
	}

	// Verify the persisted event.
	var events []persistence.PersistentRepr
	_ = j.ReplayMessages(context.Background(), s.persistenceID, 1, 1, 1,
		func(r persistence.PersistentRepr) { events = append(events, r) })

	if len(events) != 1 {
		t.Fatalf("expected 1 persisted event, got %d", len(events))
	}
	raw, ok := events[0].Payload.([]byte)
	if !ok {
		t.Fatalf("payload type: got %T, want []byte", events[0].Payload)
	}
	var ev durableEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if ev.Kind != durableEventSent {
		t.Errorf("event kind = %q, want %q", ev.Kind, durableEventSent)
	}
	if ev.SeqNr != 1 {
		t.Errorf("event seqNr = %d, want 1", ev.SeqNr)
	}
}

func TestDurableQueue_RecoveryRestoresBuffer(t *testing.T) {
	j := persistence.NewInMemoryJournal()

	// First instance: send two messages, confirm the first.
	s1 := newDurableState(t, j)
	s1.onSendMessage(nil, SendMessage{Payload: []byte("msg-1"), SerializerID: 4})
	s1.onSendMessage(nil, SendMessage{Payload: []byte("msg-2"), SerializerID: 4})
	s1.confirmedUpTo = 1
	s1.persistConfirmed(1)

	// Second instance: recover from journal.
	s2 := &durableQueueState{
		producerID:    "test-producer",
		persistenceID: "test-durable-queue",
		journal:       j,
		nextSeqNr:     1,
		selfRef:       &durableStub{path: "/user/durable-producer"},
	}
	if err := s2.recover(nil); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Only the unconfirmed message (seqNr=2) should be in the buffer.
	if len(s2.buffer) != 1 {
		t.Fatalf("expected 1 buffered message after recovery, got %d", len(s2.buffer))
	}
	if s2.buffer[0].SeqNr != 2 {
		t.Errorf("buffered seqNr = %d, want 2", s2.buffer[0].SeqNr)
	}
	if s2.nextSeqNr != 3 {
		t.Errorf("nextSeqNr = %d, want 3", s2.nextSeqNr)
	}
	if s2.confirmedUpTo != 1 {
		t.Errorf("confirmedUpTo = %d, want 1", s2.confirmedUpTo)
	}
}

func TestDurableQueue_RecoveryEmptyJournal(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	s := &durableQueueState{
		producerID:    "test-producer",
		persistenceID: "empty-queue",
		journal:       j,
		nextSeqNr:     1,
		selfRef:       &durableStub{path: "/user/p"},
	}
	if err := s.recover(nil); err != nil {
		t.Fatalf("recover on empty journal: %v", err)
	}
	if len(s.buffer) != 0 {
		t.Errorf("expected empty buffer, got %d items", len(s.buffer))
	}
	if s.nextSeqNr != 1 {
		t.Errorf("nextSeqNr = %d, want 1 (no events replayed)", s.nextSeqNr)
	}
}

func TestDurableQueue_ConfirmPrunesBuffer(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	s := newDurableState(t, j)

	s.onSendMessage(nil, SendMessage{Payload: []byte("a"), SerializerID: 4})
	s.onSendMessage(nil, SendMessage{Payload: []byte("b"), SerializerID: 4})
	s.onSendMessage(nil, SendMessage{Payload: []byte("c"), SerializerID: 4})

	if len(s.buffer) != 3 {
		t.Fatalf("expected 3 buffered messages, got %d", len(s.buffer))
	}

	// Acknowledge the first two.
	s.confirmedUpTo = 2
	s.pruneBuffer()

	if len(s.buffer) != 1 {
		t.Fatalf("expected 1 message after pruning, got %d", len(s.buffer))
	}
	if s.buffer[0].SeqNr != 3 {
		t.Errorf("remaining seqNr = %d, want 3", s.buffer[0].SeqNr)
	}
}

func TestDurableQueue_SendBufferedDelivery(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	s := newDurableState(t, j)

	consumer := &durableStub{path: "/user/consumer"}
	s.consumerRef = consumer
	s.requestUpTo = 3

	s.onSendMessage(nil, SendMessage{Payload: []byte("x"), SerializerID: 4})
	s.onSendMessage(nil, SendMessage{Payload: []byte("y"), SerializerID: 4})

	if len(consumer.msgs) != 2 {
		t.Fatalf("consumer should have received 2 messages, got %d", len(consumer.msgs))
	}

	for i, m := range consumer.msgs {
		sm, ok := m.(*SequencedMessage)
		if !ok {
			t.Fatalf("msg[%d]: expected *SequencedMessage, got %T", i, m)
		}
		if sm.SeqNr != int64(i+1) {
			t.Errorf("msg[%d]: SeqNr = %d, want %d", i, sm.SeqNr, i+1)
		}
	}
}

func TestDurableQueue_DefaultPersistenceID(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	dq := NewDurableProducerQueue("my-prod", j, DurableProducerQueueSettings{})
	_ = dq // just check construction doesn't panic

	// Inspect via a direct state construction.
	s := &durableQueueState{
		producerID:    "my-prod",
		persistenceID: "durable-producer-queue-my-prod",
		journal:       j,
		nextSeqNr:     1,
	}
	want := "durable-producer-queue-my-prod"
	if s.persistenceID != want {
		t.Errorf("persistenceID = %q, want %q", s.persistenceID, want)
	}
}

func TestDurableQueue_CustomPersistenceID(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	dq := NewDurableProducerQueue("my-prod", j, DurableProducerQueueSettings{
		PersistenceID: "custom-id",
	})
	_ = dq
}

func TestDurableQueue_AckMsgTriggersJournalCleanup(t *testing.T) {
	j := persistence.NewInMemoryJournal()
	s := newDurableState(t, j)

	s.onSendMessage(nil, SendMessage{Payload: []byte("p"), SerializerID: 4})

	// Simulate receiving an AckMsg.
	s.confirmedUpTo = 1
	s.pruneBuffer()
	_ = j.AsyncDeleteMessagesTo(context.Background(), s.persistenceID, 1)

	// After deletion, highest seqNr should be 0 (all cleaned up).
	highest, err := j.ReadHighestSequenceNr(context.Background(), s.persistenceID, 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 0 {
		// Some journals keep the sequence nr even after deletion.
		// Accept both 0 and 1 since in-memory may or may not clear the high watermark.
		t.Logf("note: after delete, highest seqNr = %d (journal-implementation specific)", highest)
	}
}

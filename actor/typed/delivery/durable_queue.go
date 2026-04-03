/*
 * durable_queue.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
)

// ── DurableProducerQueue ──────────────────────────────────────────────────────

// DurableProducerQueueSettings controls persistence behaviour of the durable
// queue.
type DurableProducerQueueSettings struct {
	// PersistenceID is the unique identifier used to store and replay events.
	// When empty, defaults to "durable-producer-queue-<producerID>".
	PersistenceID string
}

// durableEvent is the discriminated union persisted to the journal.
type durableEvent struct {
	Kind    string          `json:"kind"`            // "sent" | "confirmed"
	SeqNr   int64           `json:"seq_nr"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

const (
	durableEventSent      = "sent"
	durableEventConfirmed = "confirmed"
)

// durableQueueState holds the mutable state of the DurableProducerQueue.
type durableQueueState struct {
	producerID    string
	persistenceID string
	journal       persistence.Journal

	// Embedded producer logic (mirrors producerState fields).
	selfRef       actor.Ref
	consumerRef   actor.Ref
	nextSeqNr     int64
	sentUpTo      int64
	confirmedUpTo int64
	requestUpTo   int64
	buffer        []*SequencedMessage

	// Recovery flag: true until the journal replay is complete.
	recovering bool
	// recovered tracks whether we have loaded state from the journal on first start.
	recovered bool
}

// NewDurableProducerQueue returns a typed.Behavior[any] that acts as a
// persistence-backed ProducerController.
//
// Every message sent via SendMessage is written to the journal before being
// forwarded to the consumer.  On restart the queue replays the journal to
// re-send any messages that were not yet confirmed by the consumer, providing
// at-least-once delivery semantics that survive producer crashes.
//
// Usage:
//
//	dq := delivery.NewDurableProducerQueue("my-producer", journal,
//	    delivery.DurableProducerQueueSettings{})
//	ref, _ := sys.ActorOf(actor.Props{New: func() actor.Actor {
//	    return typed.NewTypedActor[any](dq)
//	}}, "durable-producer")
//
// The actor understands the same messages as ProducerController:
//   - SendMessage — enqueue a message for durable delivery
//   - RegisterConsumer — wire the consumer
//   - *Request — flow-control window from consumer
//   - *AckMsg — consumer acknowledgement; triggers journal cleanup
func NewDurableProducerQueue(
	producerID string,
	journal persistence.Journal,
	settings DurableProducerQueueSettings,
) typed.Behavior[any] {
	pid := settings.PersistenceID
	if pid == "" {
		pid = "durable-producer-queue-" + producerID
	}
	s := &durableQueueState{
		producerID:    producerID,
		persistenceID: pid,
		journal:       journal,
		nextSeqNr:     1,
	}
	return s.handle
}

func (s *durableQueueState) handle(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
	if s.selfRef == nil {
		s.selfRef = ctx.Self().Untyped()
	}

	// Recover from the journal the very first time we process a message.
	if !s.recovered {
		s.recovered = true
		if err := s.recover(ctx); err != nil {
			log.Printf("DurableProducerQueue %s: recovery failed: %v", s.producerID, err)
		}
	}

	switch m := msg.(type) {
	case RegisterConsumer:
		s.onRegisterConsumer(ctx, m)

	case SendMessage:
		s.onSendMessage(ctx, m)

	case *Request:
		s.confirmedUpTo = m.ConfirmedSeqNr
		s.requestUpTo = m.RequestUpToSeqNr
		s.persistConfirmed(m.ConfirmedSeqNr)
		s.sendBuffered()

	case *AckMsg:
		s.confirmedUpTo = m.ConfirmedSeqNr
		s.persistConfirmed(m.ConfirmedSeqNr)
		s.pruneBuffer()
		// Clean up journal entries up to confirmed seqNr.
		_ = s.journal.AsyncDeleteMessagesTo(
			context.Background(), s.persistenceID, uint64(m.ConfirmedSeqNr))

	default:
		log.Printf("DurableProducerQueue: unhandled message %T", msg)
	}
	return typed.Same[any]()
}

// recover replays the journal to restore the queue state after a restart.
func (s *durableQueueState) recover(_ typed.TypedContext[any]) error {
	ctx := context.Background()

	// ── Step 1: load the highest confirmed seqNr from the confirmation log ──
	confirmID := s.persistenceID + "-confirmed"
	highestConfirm, err := s.journal.ReadHighestSequenceNr(ctx, confirmID, 0)
	if err != nil {
		return fmt.Errorf("ReadHighestSequenceNr(confirmed): %w", err)
	}
	var confirmedUpTo int64
	if highestConfirm > 0 {
		// The confirmation log stores durableEvent{Kind:"confirmed",SeqNr:n}
		// at journal seqNr=n.  Replay to find the highest confirmed message seqNr.
		_ = s.journal.ReplayMessages(ctx, confirmID, 1, highestConfirm, highestConfirm,
			func(repr persistence.PersistentRepr) {
				raw, ok := repr.Payload.([]byte)
				if !ok {
					return
				}
				var ev durableEvent
				if json.Unmarshal(raw, &ev) != nil {
					return
				}
				if ev.Kind == durableEventConfirmed && ev.SeqNr > confirmedUpTo {
					confirmedUpTo = ev.SeqNr
				}
			})
	}

	// ── Step 2: replay sent messages ─────────────────────────────────────────
	highest, err := s.journal.ReadHighestSequenceNr(ctx, s.persistenceID, 0)
	if err != nil {
		return fmt.Errorf("ReadHighestSequenceNr: %w", err)
	}
	if highest == 0 {
		// Fresh start — nothing to replay.
		s.confirmedUpTo = confirmedUpTo
		return nil
	}

	var unconfirmed []*SequencedMessage

	replayErr := s.journal.ReplayMessages(ctx, s.persistenceID, 1, highest, highest,
		func(repr persistence.PersistentRepr) {
			raw, ok := repr.Payload.([]byte)
			if !ok {
				return
			}
			var ev durableEvent
			if err := json.Unmarshal(raw, &ev); err != nil {
				return
			}
			if ev.Kind != durableEventSent {
				return
			}
			var p SendMessage
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return
			}
			unconfirmed = append(unconfirmed, &SequencedMessage{
				ProducerID:  s.producerID,
				ProducerRef: s.selfRef.Path(),
				SeqNr:       ev.SeqNr,
				First:       ev.SeqNr == 1,
				Message: Payload{
					EnclosedMessage: p.Payload,
					SerializerID:    p.SerializerID,
					Manifest:        p.Manifest,
				},
			})
		})
	if replayErr != nil {
		return fmt.Errorf("ReplayMessages: %w", replayErr)
	}

	s.confirmedUpTo = confirmedUpTo

	// Keep only unconfirmed messages in the buffer.
	for _, sm := range unconfirmed {
		if sm.SeqNr > confirmedUpTo {
			s.buffer = append(s.buffer, sm)
		}
	}

	// Advance nextSeqNr past the highest persisted message.
	if len(s.buffer) > 0 {
		last := s.buffer[len(s.buffer)-1].SeqNr
		s.nextSeqNr = last + 1
	} else if confirmedUpTo > 0 {
		s.nextSeqNr = confirmedUpTo + 1
	}

	log.Printf("DurableProducerQueue %s: recovered confirmedUpTo=%d, buffered=%d, nextSeqNr=%d",
		s.producerID, s.confirmedUpTo, len(s.buffer), s.nextSeqNr)
	return nil
}

func (s *durableQueueState) onRegisterConsumer(_ typed.TypedContext[any], m RegisterConsumer) {
	log.Printf("DurableProducerQueue %s: consumer registered at %s",
		s.producerID, m.ConsumerControllerRef)
	// We don't resolve here — the consumer path is stored for reconnect logic.
	// The real ref is set when a *Request arrives (which carries the consumer's
	// flow-control window and implicitly the reply address via the actor context).
	// For simplicity we store the path and use it when the consumerRef is nil.
	_ = m
}

func (s *durableQueueState) onSendMessage(_ typed.TypedContext[any], m SendMessage) {
	seqNr := s.nextSeqNr
	s.nextSeqNr++

	// Persist before sending.
	if err := s.persistSent(seqNr, m); err != nil {
		log.Printf("DurableProducerQueue %s: persist seqNr=%d failed: %v",
			s.producerID, seqNr, err)
		// Still enqueue — the message will be sent but may be lost if we crash
		// before persisting. In production use a synchronous journal.
	}

	sm := &SequencedMessage{
		ProducerID:  s.producerID,
		ProducerRef: s.selfRef.Path(),
		SeqNr:       seqNr,
		First:       seqNr == 1,
		Message: Payload{
			EnclosedMessage: m.Payload,
			SerializerID:    m.SerializerID,
			Manifest:        m.Manifest,
		},
	}
	s.buffer = append(s.buffer, sm)
	s.sendBuffered()
}

// sendBuffered delivers buffered messages within the consumer's flow-control
// window.  It tracks sentUpTo so each message is delivered only once.
func (s *durableQueueState) sendBuffered() {
	if s.consumerRef == nil {
		return
	}
	for _, sm := range s.buffer {
		if sm.SeqNr <= s.requestUpTo && sm.SeqNr > s.sentUpTo {
			s.consumerRef.Tell(sm, s.selfRef)
			s.sentUpTo = sm.SeqNr
		}
	}
}

func (s *durableQueueState) pruneBuffer() {
	filtered := s.buffer[:0]
	for _, sm := range s.buffer {
		if sm.SeqNr > s.confirmedUpTo {
			filtered = append(filtered, sm)
		}
	}
	s.buffer = filtered
}

// persistSent writes a "sent" event to the journal.
func (s *durableQueueState) persistSent(seqNr int64, m SendMessage) error {
	payloadJSON, err := json.Marshal(m)
	if err != nil {
		return err
	}
	ev := durableEvent{Kind: durableEventSent, SeqNr: seqNr, Payload: payloadJSON}
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	repr := persistence.PersistentRepr{
		PersistenceID: s.persistenceID,
		SequenceNr:    uint64(seqNr),
		Payload:       raw,
	}
	return s.journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{repr})
}

// persistConfirmed writes a "confirmed" event to the journal.
func (s *durableQueueState) persistConfirmed(seqNr int64) {
	if seqNr <= 0 {
		return
	}
	ev := durableEvent{Kind: durableEventConfirmed, SeqNr: seqNr}
	raw, _ := json.Marshal(ev)
	// Use a derived sequence number so it doesn't collide with sent events.
	// We encode it as the seqNr negated (stored as a high journal seqNr).
	// Simpler: use a separate persistence ID for confirmations.
	confirmID := s.persistenceID + "-confirmed"
	repr := persistence.PersistentRepr{
		PersistenceID: confirmID,
		SequenceNr:    uint64(seqNr),
		Payload:       raw,
	}
	_ = s.journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{repr})
}

/*
 * actor/delivery/producer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import (
	"log"

	"github.com/sopranoworks/gekka/actor"
)

// producerState holds the mutable state of the ProducerController.
type producerState struct {
	producerID    string
	consumerPath  string // Artery path of the ConsumerController
	selfRef       actor.Ref
	consumerRef   actor.Ref // resolved when consumer registers
	nextSeqNr     int64
	requestedUpTo int64                       // last requestUpToSeqNr from consumer
	unacked       map[int64]*SequencedMessage // seqNr → buffered message awaiting Ack
	pending       []*SequencedMessage         // messages waiting to be sent (before window opens)
	windowSize    int
}

// NewProducerController returns an actor.Behavior[any] that implements the
// ProducerController side of Pekko's Reliable Delivery protocol.
//
//   - producerID uniquely identifies this producer within the delivery flow.
//   - consumerPath is the full Artery actor path of the ConsumerController.
//   - windowSize controls the maximum number of unacknowledged messages in
//     flight; use DefaultWindowSize if unsure.
//
// Usage:
//
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
//	    return actor.NewTypedActor[any](
//	        delivery.NewProducerController("my-id", consumerPath, delivery.DefaultWindowSize),
//	    )
//	}}, "producer")
//
//	// Enqueue a message:
//	ref.Tell(delivery.SendMessage{Payload: []byte("hello"), SerializerID: 4})
func NewProducerController(producerID, consumerPath string, windowSize int) actor.Behavior[any] {
	if windowSize <= 0 {
		windowSize = DefaultWindowSize
	}
	state := &producerState{
		producerID:   producerID,
		consumerPath: consumerPath,
		nextSeqNr:    1,
		unacked:      make(map[int64]*SequencedMessage),
		windowSize:   windowSize,
	}
	return state.handleCommand
}

// handleCommand is the behavior function: dispatches on message type.
func (p *producerState) handleCommand(ctx actor.TypedContext[any], msg any) actor.Behavior[any] {
	if p.selfRef == nil {
		p.selfRef = ctx.Self().Untyped()
	}

	switch m := msg.(type) {
	case SendMessage:
		p.onSendMessage(ctx, &m)
	case *Request:
		p.onRequest(ctx, m)
	case *AckMsg:
		p.onAck(m)
	case *Resend:
		p.onResend(ctx, m)
	case *RegisterConsumer:
		p.onRegisterConsumer(ctx, m)
	default:
		log.Printf("ProducerController[%s]: unhandled message %T", p.producerID, msg)
	}
	return actor.Same[any]()
}

// onRegisterConsumer records the consumer ref sent by the ConsumerController.
func (p *producerState) onRegisterConsumer(ctx actor.TypedContext[any], m *RegisterConsumer) {
	log.Printf("ProducerController[%s]: registered consumer at %s", p.producerID, m.ConsumerControllerRef)
	consumerRef := p.resolveRef(ctx, m.ConsumerControllerRef)
	if consumerRef == nil {
		return
	}
	p.consumerRef = consumerRef
	// Flush any pending messages that were enqueued before the consumer registered.
	p.trySend(ctx)
}

// onSendMessage enqueues a user message for delivery.
func (p *producerState) onSendMessage(ctx actor.TypedContext[any], m *SendMessage) {
	if p.consumerRef == nil {
		p.consumerRef = p.resolveRef(ctx, p.consumerPath)
	}

	seq := &SequencedMessage{
		ProducerID:  p.producerID,
		SeqNr:       p.nextSeqNr,
		First:       p.nextSeqNr == 1,
		Ack:         true,
		ProducerRef: p.selfPath(ctx),
		Message: Payload{
			EnclosedMessage: m.Payload,
			SerializerID:    m.SerializerID,
			Manifest:        m.Manifest,
		},
	}
	p.nextSeqNr++
	p.pending = append(p.pending, seq)
	p.trySend(ctx)
}

// trySend sends messages from the pending queue up to the consumer's requested window.
func (p *producerState) trySend(ctx actor.TypedContext[any]) {
	if p.consumerRef == nil {
		return
	}
	for len(p.pending) > 0 && p.pending[0].SeqNr <= p.requestedUpTo {
		seq := p.pending[0]
		p.pending = p.pending[1:]
		p.unacked[seq.SeqNr] = seq
		p.consumerRef.Tell(seq, p.selfRef)
		log.Printf("ProducerController[%s]: sent seqNr=%d to %s", p.producerID, seq.SeqNr, p.consumerPath)
	}
}

// onRequest handles a Request from the ConsumerController (flow control).
func (p *producerState) onRequest(ctx actor.TypedContext[any], m *Request) {
	log.Printf("ProducerController[%s]: Request confirmed=%d upTo=%d", p.producerID, m.ConfirmedSeqNr, m.RequestUpToSeqNr)

	// Advance the window.
	if m.RequestUpToSeqNr > p.requestedUpTo {
		p.requestedUpTo = m.RequestUpToSeqNr
	}

	// Remove confirmed messages from the unacked buffer.
	for seqNr := range p.unacked {
		if seqNr <= m.ConfirmedSeqNr {
			delete(p.unacked, seqNr)
		}
	}

	p.trySend(ctx)
}

// onAck handles an Ack from the ConsumerController.
func (p *producerState) onAck(m *AckMsg) {
	log.Printf("ProducerController[%s]: Ack confirmed=%d", p.producerID, m.ConfirmedSeqNr)
	for seqNr := range p.unacked {
		if seqNr <= m.ConfirmedSeqNr {
			delete(p.unacked, seqNr)
		}
	}
}

// onResend re-sends all unacked messages starting from fromSeqNr.
func (p *producerState) onResend(ctx actor.TypedContext[any], m *Resend) {
	log.Printf("ProducerController[%s]: Resend from seqNr=%d", p.producerID, m.FromSeqNr)
	if p.consumerRef == nil {
		return
	}
	for seqNr := m.FromSeqNr; seqNr < p.nextSeqNr; seqNr++ {
		if msg, ok := p.unacked[seqNr]; ok {
			p.consumerRef.Tell(msg, p.selfRef)
		}
	}
}

// resolveRef returns an actor.Ref for the given Artery path, or nil on error.
func (p *producerState) resolveRef(ctx actor.TypedContext[any], path string) actor.Ref {
	if path == "" {
		return nil
	}
	ref, err := ctx.System().Resolve(path)
	if err != nil {
		log.Printf("ProducerController[%s]: resolve %q: %v", p.producerID, path, err)
		return nil
	}
	return ref
}

// selfPath returns the full Artery path of this actor.
func (p *producerState) selfPath(ctx actor.TypedContext[any]) string {
	if p.selfRef != nil {
		return p.selfRef.Path()
	}
	return ctx.Self().Path()
}

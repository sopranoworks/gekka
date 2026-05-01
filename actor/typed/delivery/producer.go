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
	"github.com/sopranoworks/gekka/actor/typed"
)

// producerState holds the mutable state of the ProducerController.
type producerState struct {
	producerID    string
	selfRef       actor.Ref
	consumerRef   actor.Ref
	consumerPath  string
	nextSeqNr     int64
	sentUpTo      int64
	confirmedUpTo int64
	requestUpTo   int64
	// chunkSize mirrors pekko.reliable-delivery.producer-controller.chunk-large-messages.
	// 0 = chunking disabled.
	chunkSize int
	// buffer holds messages waiting for window space or confirmation.
	buffer []*SequencedMessage
}

// NewProducerController returns an typed.Behavior[any] that implements the
// ProducerController side of Pekko's Reliable Delivery protocol.
//
// Usage:
//
//	pc := delivery.NewProducerController("producer-1")
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
//	    return typed.NewTypedActor[any](pc)
//	}}, "producer")
func NewProducerController(producerID string) typed.Behavior[any] {
	state := &producerState{
		producerID: producerID,
		nextSeqNr:  1,
	}
	return state.handleCommand
}

// NewProducerControllerFromConfig is the HOCON-driven counterpart of
// NewProducerController. It honours
// pekko.reliable-delivery.producer-controller.* values: chunk-large-messages
// drives the byte threshold above which payloads are split into chunked
// SequencedMessages.
func NewProducerControllerFromConfig(producerID string, cfg ProducerControllerConfig) typed.Behavior[any] {
	state := &producerState{
		producerID: producerID,
		nextSeqNr:  1,
		chunkSize:  cfg.ChunkLargeMessages,
	}
	return state.handleCommand
}

func (p *producerState) handleCommand(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
	if p.selfRef == nil {
		p.selfRef = ctx.Self().Untyped()
	}

	switch m := msg.(type) {
	case RegisterConsumer:
		p.onRegisterConsumer(ctx, m)
	case *RegisterConsumer:
		p.onRegisterConsumer(ctx, *m)
	case *Request:
		p.onRequest(ctx, m)
	case *AckMsg:
		p.onAck(ctx, m)
	case SendMessage:
		p.onSendMessage(ctx, m)
	default:
		log.Printf("ProducerController: unhandled message %T", msg)
	}
	return typed.Same[any]()
}

func (p *producerState) onRegisterConsumer(ctx typed.TypedContext[any], m RegisterConsumer) {
	log.Printf("ProducerController: consumer registered at %s", m.ConsumerControllerRef)
	ref, err := ctx.System().Resolve(m.ConsumerControllerRef)
	if err != nil {
		log.Printf("ProducerController: resolve consumer %q: %v", m.ConsumerControllerRef, err)
		return
	}
	p.consumerRef = ref
	p.consumerPath = m.ConsumerControllerRef
	// After consumer registration, attempt to send buffered messages.
	// The first message (First=true) is sent immediately to initiate the protocol.
	p.sendBuffered(ctx)
}

func (p *producerState) onRequest(ctx typed.TypedContext[any], m *Request) {
	p.confirmedUpTo = m.ConfirmedSeqNr
	p.requestUpTo = m.RequestUpToSeqNr
	p.sendBuffered(ctx)
}

func (p *producerState) onAck(_ typed.TypedContext[any], m *AckMsg) {
	p.confirmedUpTo = m.ConfirmedSeqNr
	// Prune buffer
	newBuffer := p.buffer[:0]
	for _, msg := range p.buffer {
		if msg.SeqNr > p.confirmedUpTo {
			newBuffer = append(newBuffer, msg)
		}
	}
	p.buffer = newBuffer
}

func (p *producerState) onSendMessage(ctx typed.TypedContext[any], m SendMessage) {
	// Chunking: when chunkSize is configured and the payload exceeds it,
	// split into multiple SequencedMessages with HasChunk=true. The first
	// chunk gets FirstChunk=true and the last LastChunk=true. Each chunk
	// receives its own SeqNr (the consumer reassembles them on arrival).
	if p.chunkSize > 0 && len(m.Payload) > p.chunkSize {
		p.appendChunkedMessages(m)
		p.sendBuffered(ctx)
		return
	}

	seqMsg := &SequencedMessage{
		ProducerRef: p.selfRef.Path(),
		SeqNr:       p.nextSeqNr,
		Message: Payload{
			EnclosedMessage: m.Payload,
			SerializerID:    m.SerializerID,
			Manifest:        m.Manifest,
		},
		First: p.nextSeqNr == 1,
	}
	p.nextSeqNr++
	p.buffer = append(p.buffer, seqMsg)
	p.sendBuffered(ctx)
}

// appendChunkedMessages splits a SendMessage payload into chunks of at
// most p.chunkSize bytes and appends each as its own SequencedMessage
// to the producer buffer.
func (p *producerState) appendChunkedMessages(m SendMessage) {
	payload := m.Payload
	total := len(payload)
	for offset := 0; offset < total; offset += p.chunkSize {
		end := offset + p.chunkSize
		if end > total {
			end = total
		}
		chunk := payload[offset:end]
		seqMsg := &SequencedMessage{
			ProducerRef: p.selfRef.Path(),
			SeqNr:       p.nextSeqNr,
			Message: Payload{
				EnclosedMessage: chunk,
				SerializerID:    m.SerializerID,
				Manifest:        m.Manifest,
			},
			First:      p.nextSeqNr == 1,
			HasChunk:   true,
			FirstChunk: offset == 0,
			LastChunk:  end == total,
		}
		p.nextSeqNr++
		p.buffer = append(p.buffer, seqMsg)
	}
}

func (p *producerState) sendBuffered(_ typed.TypedContext[any]) {
	if p.consumerRef == nil {
		return
	}
	for _, msg := range p.buffer {
		if msg.SeqNr > p.sentUpTo {
			// The first message (First=true) is always sent to initiate the
			// protocol — the consumer won't send a Request until it receives
			// this initial message.  Subsequent messages respect the flow
			// control window (requestUpTo).
			if msg.First || msg.SeqNr <= p.requestUpTo {
				p.consumerRef.Tell(msg, p.selfRef)
				p.sentUpTo = msg.SeqNr
			}
		}
	}
}

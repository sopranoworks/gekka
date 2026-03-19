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
	producerID   string
	selfRef      actor.Ref
	consumerRef  actor.Ref
	consumerPath string
	nextSeqNr    int64
	sentUpTo     int64
	confirmedUpTo int64
	requestUpTo  int64
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

func (p *producerState) handleCommand(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
	if p.selfRef == nil {
		p.selfRef = ctx.Self().Untyped()
	}

	switch m := msg.(type) {
	case RegisterConsumer:
		p.onRegisterConsumer(ctx, m)
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

func (p *producerState) sendBuffered(_ typed.TypedContext[any]) {
	if p.consumerRef == nil {
		return
	}
	for _, msg := range p.buffer {
		if msg.SeqNr <= p.requestUpTo && msg.SeqNr > p.sentUpTo {
			p.consumerRef.Tell(msg, p.selfRef)
			p.sentUpTo = msg.SeqNr
		}
	}
}

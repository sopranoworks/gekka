/*
 * actor/delivery/consumer.go
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

// consumerState holds the mutable state of the ConsumerController.
type consumerState struct {
	consumerActor  actor.Ref // the application actor that receives Delivery messages
	selfRef        actor.Ref
	producerRef    actor.Ref // resolved from SequencedMessage.ProducerRef
	producerPath   string    // path used to resolve producerRef
	expectedSeqNr  int64
	confirmedSeqNr int64
	requestedUpTo  int64
	windowSize     int
	// onlyFlowControl mirrors pekko.reliable-delivery.consumer-controller.only-flow-control.
	// When true, gaps in SeqNr are accepted silently (no Resend request is
	// sent to the producer). Lost messages are not redelivered.
	onlyFlowControl bool
	// stash holds out-of-order messages received before confirmations catch up.
	stash map[int64]*SequencedMessage
	// chunkBuffer assembles chunked messages.
	chunkBuffer []byte
	chunkStart  int64 // SeqNr of the first chunk
}

// NewConsumerController returns an typed.Behavior[any] that implements the
// ConsumerController side of Pekko's Reliable Delivery protocol.
//
//   - consumerActor is where Delivery messages are forwarded.  The consumer
//     must reply with Confirmed{SeqNr} to advance the delivery window.
//   - windowSize controls how many messages the controller requests at once.
//
// Spawn and register the ConsumerController before Pekko's ProducerController
// starts sending, so the actor path is available.
//
// Usage:
//
//	cc := delivery.NewConsumerController(myActor, delivery.DefaultWindowSize)
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
//	    return typed.NewTypedActor[any](cc)
//	}}, "consumer")
func NewConsumerController(consumerActor actor.Ref, windowSize int) typed.Behavior[any] {
	if windowSize <= 0 {
		windowSize = DefaultWindowSize
	}
	state := &consumerState{
		consumerActor: consumerActor,
		expectedSeqNr: 1,
		windowSize:    windowSize,
		stash:         make(map[int64]*SequencedMessage),
	}
	return state.handleCommand
}

// NewConsumerControllerFromConfig is the HOCON-driven counterpart of
// NewConsumerController. It honours pekko.reliable-delivery.consumer-controller.*
// values: flow-control-window seeds windowSize, only-flow-control suppresses
// the gap-filling Resend behaviour. Zero/negative FlowControlWindow falls
// back to DefaultWindowSize.
func NewConsumerControllerFromConfig(consumerActor actor.Ref, cfg ConsumerControllerConfig) typed.Behavior[any] {
	window := cfg.FlowControlWindow
	if window <= 0 {
		window = DefaultWindowSize
	}
	state := &consumerState{
		consumerActor:   consumerActor,
		expectedSeqNr:   1,
		windowSize:      window,
		onlyFlowControl: cfg.OnlyFlowControl,
		stash:           make(map[int64]*SequencedMessage),
	}
	return state.handleCommand
}

// handleCommand dispatches on message type.
func (c *consumerState) handleCommand(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
	if c.selfRef == nil {
		c.selfRef = ctx.Self().Untyped()
		// Register with the producer by resolving its path (done reactively on first message).
	}

	switch m := msg.(type) {
	case ConsumerStart:
		c.onConsumerStart(ctx, m)
	case *SequencedMessage:
		c.onSequencedMessage(ctx, m)
	case Confirmed:
		c.onConfirmed(ctx, m)
	default:
		log.Printf("ConsumerController: unhandled message %T", msg)
	}
	return typed.Same[any]()
}

// onConsumerStart handles a ConsumerStart command: registers with the given producer.
func (c *consumerState) onConsumerStart(ctx typed.TypedContext[any], m ConsumerStart) {
	log.Printf("ConsumerController: registering with producer at %s", m.ProducerPath)
	ref, err := ctx.System().Resolve(m.ProducerPath)
	if err != nil {
		log.Printf("ConsumerController: resolve producer %q: %v", m.ProducerPath, err)
		return
	}
	c.producerRef = ref
	c.producerPath = m.ProducerPath
	// Send RegisterConsumer so the producer knows our path.
	c.producerRef.Tell(&RegisterConsumer{ConsumerControllerRef: c.selfRef.Path()}, c.selfRef)
	// Open the initial delivery window.
	c.sendRequest(ctx, false)
}

// onSequencedMessage processes an incoming SequencedMessage from the producer.
func (c *consumerState) onSequencedMessage(ctx typed.TypedContext[any], m *SequencedMessage) {
	// Resolve producer ref on first message.
	if c.producerRef == nil && m.ProducerRef != "" && m.ProducerRef != c.producerPath {
		c.producerPath = m.ProducerRef
		ref, err := ctx.System().Resolve(m.ProducerRef)
		if err != nil {
			log.Printf("ConsumerController: resolve producer %q: %v", m.ProducerRef, err)
		} else {
			c.producerRef = ref
			// Send initial Request to open the delivery window.
			c.sendRequest(ctx, false)
		}
	}

	if m.First {
		// Reset state when producer signals first message (re-connection).
		c.expectedSeqNr = m.SeqNr
		c.confirmedSeqNr = m.SeqNr - 1
	}

	if m.SeqNr < c.expectedSeqNr {
		// Duplicate or old message — just re-send Ack.
		c.sendAck()
		return
	}

	if m.SeqNr > c.expectedSeqNr {
		if c.onlyFlowControl {
			// only-flow-control: drop the gap silently. Skip ahead so
			// flow control still operates without redelivering lost
			// messages. Pekko does the same when only-flow-control=true.
			c.expectedSeqNr = m.SeqNr
			c.confirmedSeqNr = m.SeqNr - 1
		} else {
			// Out-of-order: stash and request resend.
			c.stash[m.SeqNr] = m
			if c.producerRef != nil {
				c.producerRef.Tell(&Resend{FromSeqNr: c.expectedSeqNr}, c.selfRef)
			}
			return
		}
	}

	// In-order: deliver to consumer.
	c.deliverMessage(ctx, m)
	c.expectedSeqNr++

	// Deliver any stashed messages that are now in order.
	for {
		next, ok := c.stash[c.expectedSeqNr]
		if !ok {
			break
		}
		delete(c.stash, c.expectedSeqNr)
		c.deliverMessage(ctx, next)
		c.expectedSeqNr++
	}

	// Request more messages when the window is half consumed.
	remaining := c.requestedUpTo - c.confirmedSeqNr
	if remaining <= int64(c.windowSize/2) {
		c.sendRequest(ctx, false)
	}
}

// deliverMessage handles chunked and regular messages.
func (c *consumerState) deliverMessage(ctx typed.TypedContext[any], m *SequencedMessage) {
	if m.HasChunk {
		if m.FirstChunk {
			c.chunkBuffer = append([]byte(nil), m.Message.EnclosedMessage...)
			c.chunkStart = m.SeqNr
		} else {
			c.chunkBuffer = append(c.chunkBuffer, m.Message.EnclosedMessage...)
		}
		if !m.LastChunk {
			return // wait for more chunks
		}
		// Reassembled: deliver the complete payload.
		payload := Payload{
			EnclosedMessage: c.chunkBuffer,
			SerializerID:    m.Message.SerializerID,
			Manifest:        m.Message.Manifest,
		}
		c.chunkBuffer = nil
		c.deliverToConsumer(ctx, m.SeqNr, payload)
	} else {
		c.deliverToConsumer(ctx, m.SeqNr, m.Message)
	}
}

// deliverToConsumer forwards a complete message to the application consumer actor.
func (c *consumerState) deliverToConsumer(_ typed.TypedContext[any], seqNr int64, payload Payload) {
	d := Delivery{
		SeqNr:     seqNr,
		Msg:       payload,
		ConfirmTo: c.selfRef,
	}
	c.consumerActor.Tell(d, c.selfRef)
}

// onConfirmed handles a Confirmed reply from the consumer actor.
func (c *consumerState) onConfirmed(_ typed.TypedContext[any], m Confirmed) {
	if m.SeqNr > c.confirmedSeqNr {
		c.confirmedSeqNr = m.SeqNr
	}
	c.sendAck()
}

// sendAck sends an Ack to the producer.
func (c *consumerState) sendAck() {
	if c.producerRef == nil {
		return
	}
	c.producerRef.Tell(&AckMsg{ConfirmedSeqNr: c.confirmedSeqNr}, c.selfRef)
}

// sendRequest sends a Request to the producer controller to open/advance the window.
func (c *consumerState) sendRequest(_ typed.TypedContext[any], viaTimeout bool) {
	if c.producerRef == nil {
		return
	}
	newUpTo := c.confirmedSeqNr + int64(c.windowSize)
	if newUpTo > c.requestedUpTo {
		c.requestedUpTo = newUpTo
	}
	req := &Request{
		ConfirmedSeqNr:   c.confirmedSeqNr,
		RequestUpToSeqNr: c.requestedUpTo,
		SupportResend:    true,
		ViaTimeout:       viaTimeout,
	}
	c.producerRef.Tell(req, c.selfRef)
	log.Printf("ConsumerController: Request confirmed=%d upTo=%d", req.ConfirmedSeqNr, req.RequestUpToSeqNr)
}

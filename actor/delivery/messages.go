/*
 * actor/delivery/messages.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package delivery implements Pekko's Reliable Delivery protocol, providing
// at-least-once delivery semantics between Go and Scala/Pekko actors.
//
// The wire format is compatible with Pekko's ReliableDeliverySerializer (ID 36).
//
// # Basic usage (Go producer → Scala consumer)
//
//	// Register the serializer on startup.
//	node.RegisterSerializer(delivery.NewSerializer())
//
//	// Spawn a ProducerController.
//	pc := delivery.NewProducerController("my-producer", consumerPath, 10)
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
//	    return actor.NewTypedActor[any](pc)
//	}}, "producer")
//
//	// Send a message.
//	ref.Tell(delivery.SendMessage{Payload: []byte("hello"), SerializerID: 4})
//
// # Basic usage (Scala producer → Go consumer)
//
//	// Spawn a ConsumerController; forward deliveries to your actor.
//	cc := delivery.NewConsumerController(myActorRef, 10)
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
//	    return actor.NewTypedActor[any](cc)
//	}}, "consumer")
package delivery

import "github.com/sopranoworks/gekka/actor"

// ReliableDeliverySerializerID is the Artery serializer ID for Pekko's
// ReliableDeliverySerializer, as defined in cluster-typed/reference.conf.
const ReliableDeliverySerializerID int32 = 36

// Wire-format manifest strings (lowercase single letters, matching Pekko's
// ReliableDeliverySerializer manifest constants).
const (
	ManifestSequencedMessage = "a"
	ManifestAck              = "b"
	ManifestRequest          = "c"
	ManifestResend           = "d"
	ManifestRegisterConsumer = "e"
)

// DefaultWindowSize is the number of in-flight messages the consumer requests
// at a time.
const DefaultWindowSize = 10

// ChunkThreshold is the payload size above which the producer splits a message
// into chunks, matching Pekko's default of 128 KiB.
const ChunkThreshold = 128 * 1024

// ---------------------------------------------------------------------------
// Wire-format message types (sent over the Artery network)
// ---------------------------------------------------------------------------

// Payload is the enclosed message inside a SequencedMessage, mirroring the
// ContainerFormats.proto Payload message used by Pekko.
type Payload struct {
	EnclosedMessage []byte
	SerializerID    int32
	Manifest        string // optional; empty string means no manifest
}

// SequencedMessage is sent from ProducerController to ConsumerController.
// Corresponds to manifest "a" in Pekko's ReliableDeliverySerializer.
type SequencedMessage struct {
	ProducerID  string
	SeqNr       int64
	First       bool
	Ack         bool   // producer requests an Ack reply after this message
	ProducerRef string // full Artery actor path of the producer controller
	Message     Payload

	// Chunking fields: present only when payload is chunked.
	HasChunk   bool
	FirstChunk bool
	LastChunk  bool
}

func (*SequencedMessage) ArterySerializerID() int32 { return ReliableDeliverySerializerID }
func (*SequencedMessage) ArteryManifest() string    { return ManifestSequencedMessage }

// AckMsg is sent from ConsumerController to ProducerController to confirm
// delivery of a sequence number.  Manifest "b".
type AckMsg struct {
	ConfirmedSeqNr int64
}

func (*AckMsg) ArterySerializerID() int32 { return ReliableDeliverySerializerID }
func (*AckMsg) ArteryManifest() string    { return ManifestAck }

// Request is sent from ConsumerController to ProducerController to request
// delivery of messages up to requestUpToSeqNr.  Manifest "c".
type Request struct {
	ConfirmedSeqNr   int64
	RequestUpToSeqNr int64
	SupportResend    bool
	ViaTimeout       bool
}

func (*Request) ArterySerializerID() int32 { return ReliableDeliverySerializerID }
func (*Request) ArteryManifest() string    { return ManifestRequest }

// Resend is sent from ConsumerController to ProducerController to request
// re-transmission starting from fromSeqNr.  Manifest "d".
type Resend struct {
	FromSeqNr int64
}

func (*Resend) ArterySerializerID() int32 { return ReliableDeliverySerializerID }
func (*Resend) ArteryManifest() string    { return ManifestResend }

// RegisterConsumer is sent from ConsumerController to ProducerController to
// register the consumer so the producer knows where to send messages.
// Manifest "e".
type RegisterConsumer struct {
	ConsumerControllerRef string // full Artery actor path
}

func (*RegisterConsumer) ArterySerializerID() int32 { return ReliableDeliverySerializerID }
func (*RegisterConsumer) ArteryManifest() string    { return ManifestRegisterConsumer }

// ---------------------------------------------------------------------------
// Application-level types (not sent over the wire)
// ---------------------------------------------------------------------------

// SendMessage is sent by application code to the ProducerController actor to
// enqueue a message for reliable delivery to the consumer.
type SendMessage struct {
	Payload      []byte
	SerializerID int32
	Manifest     string
}

// ConsumerStart is sent to a ConsumerController to initiate registration with a
// remote ProducerController at the given Artery path.  Use this when Go is the
// consumer and Scala/Pekko is the producer (Scala→Go direction), so the
// ConsumerController proactively sends RegisterConsumer to the producer.
type ConsumerStart struct {
	ProducerPath string // full Artery path of the remote ProducerController
}

// Delivery is sent by the ConsumerController to the consumer actor when a
// message arrives in order.  The consumer must call ConfirmTo.Tell(Confirmed{})
// to advance the delivery window.
type Delivery struct {
	SeqNr     int64
	Msg       Payload
	ConfirmTo actor.Ref // ConsumerController ref; send Confirmed{SeqNr} here
}

// Confirmed is sent by the consumer actor to the ConsumerController to
// acknowledge processing of the message identified by SeqNr.
type Confirmed struct {
	SeqNr int64
}

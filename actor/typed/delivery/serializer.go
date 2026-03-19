/*
 * actor/delivery/serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import "fmt"

// Serializer implements the core.Serializer interface for Pekko's
// ReliableDelivery protocol (serializer ID 36).
//
// Register it with a Cluster node before starting reliable delivery:
//
//	node.RegisterSerializer(delivery.NewSerializer())
type Serializer struct{}

// NewSerializer creates a new ReliableDeliverySerializer.
func NewSerializer() *Serializer { return &Serializer{} }

// Identifier returns the Artery serializer ID used by Pekko's
// ReliableDeliverySerializer (36), as declared in cluster-typed/reference.conf.
func (*Serializer) Identifier() int32 { return ReliableDeliverySerializerID }

// ToBinary serializes a delivery message to its proto2 binary representation.
// The manifest field on the Artery frame determines the type; each message type
// returns its own manifest via ArteryManifest().
// ToBinary serializes a delivery message to its proto2 binary representation.
func (*Serializer) ToBinary(msg any) ([]byte, error) {
	switch m := msg.(type) {
	case *SequencedMessage:
		return EncodeSequencedMessage(m), nil
	case *AckMsg:
		return EncodeAck(m), nil
	case *Request:
		return EncodeRequest(m), nil
	case *Resend:
		return EncodeResend(m), nil
	case *RegisterConsumer:
		return EncodeRegisterConsumer(m), nil
	default:
		return nil, fmt.Errorf("delivery.Serializer: unknown message type %T", msg)
	}
}

// FromBinary deserializes bytes into the delivery message type identified by
// manifest (one of "a"–"e").
// FromBinary deserializes bytes into the delivery message type identified by manifest.
func (*Serializer) FromBinary(data []byte, manifest string) (any, error) {
	switch manifest {
	case ManifestSequencedMessage: // "a"
		return DecodeSequencedMessage(data)
	case ManifestAck: // "b"
		return DecodeAck(data)
	case ManifestRequest: // "c"
		return DecodeRequest(data)
	case ManifestResend: // "d"
		return DecodeResend(data)
	case ManifestRegisterConsumer: // "e"
		return DecodeRegisterConsumer(data)
	default:
		return nil, fmt.Errorf("delivery.Serializer: unknown manifest %q", manifest)
	}
}

/*
 * misc_serializer.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"fmt"
	"log/slog"
)

// MiscMessageSerializer handles Pekko's MiscMessageSerializer (ID=16) messages.
// This includes Identify, ActorIdentity, PoisonPill, Status$Success/Failure,
// and other system messages.
//
// Gekka does not need to fully deserialize all of these message types; the
// serializer gracefully handles unknown manifests by returning an opaque wrapper
// so that the frame handler does not crash the connection.
type MiscMessageSerializer struct{}

// MiscMessage is an opaque wrapper for deserialized MiscMessageSerializer payloads.
type MiscMessage struct {
	Manifest string
	Data     []byte
}

func (*MiscMessageSerializer) Identifier() int32 { return MiscMessageSerializerID }

func (*MiscMessageSerializer) ToBinary(msg interface{}) ([]byte, error) {
	switch m := msg.(type) {
	case *MiscMessage:
		return m.Data, nil
	default:
		return nil, fmt.Errorf("MiscMessageSerializer: unsupported type %T", msg)
	}
}

func (*MiscMessageSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	slog.Debug("MiscMessageSerializer: received message", "manifest", manifest, "len", len(data))
	return &MiscMessage{Manifest: manifest, Data: data}, nil
}

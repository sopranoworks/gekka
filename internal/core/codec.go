/*
 * codec.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"io"
)

// ZeroCopySerializer is an interface for serializing messages with minimal allocations.
type ZeroCopySerializer interface {
	Serializer

	// WriteTo serializes the message directly to an io.Writer.
	// Returns the number of bytes written and any error encountered.
	WriteTo(w io.Writer, msg interface{}) (int64, error)

	// MarshalTo serializes the message into a pre-allocated buffer slice.
	// Returns the number of bytes written and any error encountered.
	// If the buffer is too small, it should return an error.
	MarshalTo(buf []byte, msg interface{}) (int, error)

	// Size returns the size of the serialized message in bytes.
	Size(msg interface{}) int
}

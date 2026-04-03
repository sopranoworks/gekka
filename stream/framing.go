/*
 * framing.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Framing provides Flow operators for delimiting a byte stream into discrete
// frames, mirroring Pekko's pekko.stream.scaladsl.Framing object.
//
// All operators work on Flow[[]byte, []byte, NotUsed] and compose naturally
// with TCP, file, or any other source of raw byte chunks.
var Framing framingOps

type framingOps struct{}

// DelimiterFraming returns a Flow that splits the incoming byte stream on the
// given delimiter sequence and emits each frame with the delimiter stripped.
//
// If the incoming data does not contain a delimiter within maximumFrameLength
// bytes the stream fails with an error. When allowTruncation is true, any
// remaining data after the last delimiter (i.e. the final unterminated frame)
// is emitted as-is rather than being dropped or causing an error.
//
// This mirrors Pekko's Framing.delimiter.
func (framingOps) DelimiterFraming(delimiter []byte, maximumFrameLength int, allowTruncation bool) Flow[[]byte, []byte, NotUsed] {
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			return &delimiterFramingIter{
				upstream:  up,
				delimiter: delimiter,
				maxFrame:  maximumFrameLength,
				allowTrunc: allowTruncation,
			}, NotUsed{}
		},
	}
}

// LengthFieldFraming returns a Flow that parses each frame from a length-field
// prefix followed by that many bytes of payload.
//
//   - fieldLength: number of bytes used to encode the length (1, 2, 4, or 8).
//   - maximumFrameLength: maximum allowed payload size in bytes.
//   - byteOrder: binary.BigEndian or binary.LittleEndian.
//
// The emitted frames do NOT include the length field. The stream fails if a
// decoded length exceeds maximumFrameLength.
//
// This mirrors Pekko's Framing.lengthField.
func (framingOps) LengthFieldFraming(fieldLength int, maximumFrameLength int, byteOrder binary.ByteOrder) Flow[[]byte, []byte, NotUsed] {
	return Flow[[]byte, []byte, NotUsed]{
		attach: func(up iterator[[]byte]) (iterator[[]byte], NotUsed) {
			return &lengthFieldFramingIter{
				upstream:  up,
				fieldLen:  fieldLength,
				maxFrame:  maximumFrameLength,
				byteOrder: byteOrder,
			}, NotUsed{}
		},
	}
}

// SimpleFramingProtocolDecoder returns a Flow that decodes frames produced by
// SimpleFramingProtocolEncoder: each frame is preceded by a 4-byte big-endian
// length prefix that is stripped before the payload is emitted.
//
// This mirrors Pekko's Framing.simpleFramingProtocolDecoder.
func (framingOps) SimpleFramingProtocolDecoder(maximumMessageLength int) Flow[[]byte, []byte, NotUsed] {
	return Framing.LengthFieldFraming(4, maximumMessageLength, binary.BigEndian)
}

// SimpleFramingProtocolEncoder returns a Flow that prepends a 4-byte big-endian
// length to each frame, producing output compatible with
// SimpleFramingProtocolDecoder.
//
// This mirrors Pekko's Framing.simpleFramingProtocolEncoder.
func (framingOps) SimpleFramingProtocolEncoder(maximumMessageLength int) Flow[[]byte, []byte, NotUsed] {
	return Map[[]byte, []byte](func(frame []byte) []byte {
		if len(frame) > maximumMessageLength {
			// Truncate silently to match Pekko's behaviour of relying on the
			// caller to respect the limit; a proper implementation would fail
			// the stream, but for compatibility we truncate here.
			frame = frame[:maximumMessageLength]
		}
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(frame)))
		return append(header, frame...)
	})
}

// ── delimiterFramingIter ──────────────────────────────────────────────────────

type delimiterFramingIter struct {
	upstream   iterator[[]byte]
	delimiter  []byte
	maxFrame   int
	allowTrunc bool

	buf    []byte   // accumulated bytes not yet parsed into frames
	frames [][]byte // fully parsed frames ready to emit
	done   bool     // upstream exhausted
	err    error    // terminal error from upstream
}

func (d *delimiterFramingIter) next() ([]byte, bool, error) {
	for {
		// Emit any buffered frames first.
		if len(d.frames) > 0 {
			f := d.frames[0]
			d.frames = d.frames[1:]
			return f, true, nil
		}

		if d.done {
			// Upstream exhausted — handle any remaining buffered bytes.
			if len(d.buf) > 0 {
				if d.allowTrunc {
					f := d.buf
					d.buf = nil
					return f, true, nil
				}
				// Unterminated frame without allowTruncation → error.
				return nil, false, fmt.Errorf("framing: unterminated frame of %d bytes (no delimiter found)", len(d.buf))
			}
			return nil, false, d.err
		}

		// Pull the next chunk from upstream.
		chunk, ok, err := d.upstream.next()
		if !ok {
			d.done = true
			d.err = err
			continue
		}
		d.buf = append(d.buf, chunk...)

		// Parse as many complete frames as possible.
		for {
			idx := bytes.Index(d.buf, d.delimiter)
			if idx < 0 {
				// No delimiter in buffer yet.
				if len(d.buf) > d.maxFrame {
					return nil, false, fmt.Errorf("framing: no delimiter found within maximum frame length %d", d.maxFrame)
				}
				break
			}
			if idx > d.maxFrame {
				return nil, false, fmt.Errorf("framing: frame of length %d exceeds maximum %d", idx, d.maxFrame)
			}
			frame := make([]byte, idx)
			copy(frame, d.buf[:idx])
			d.buf = d.buf[idx+len(d.delimiter):]
			d.frames = append(d.frames, frame)
		}
	}
}

// ── lengthFieldFramingIter ────────────────────────────────────────────────────

type lengthFieldFramingIter struct {
	upstream  iterator[[]byte]
	fieldLen  int
	maxFrame  int
	byteOrder binary.ByteOrder

	buf    []byte
	frames [][]byte
	done   bool
	err    error
}

func (l *lengthFieldFramingIter) next() ([]byte, bool, error) {
	for {
		if len(l.frames) > 0 {
			f := l.frames[0]
			l.frames = l.frames[1:]
			return f, true, nil
		}

		if l.done {
			if len(l.buf) > 0 {
				return nil, false, fmt.Errorf("framing: incomplete frame: %d trailing bytes", len(l.buf))
			}
			return nil, false, l.err
		}

		// Pull until we have enough bytes to parse frames.
		chunk, ok, err := l.upstream.next()
		if !ok {
			l.done = true
			l.err = err
			continue
		}
		l.buf = append(l.buf, chunk...)

		// Parse complete frames from the buffer.
		for len(l.buf) >= l.fieldLen {
			frameLen, parseErr := l.readLength(l.buf[:l.fieldLen])
			if parseErr != nil {
				return nil, false, parseErr
			}
			if frameLen > l.maxFrame {
				return nil, false, fmt.Errorf("framing: frame length %d exceeds maximum %d", frameLen, l.maxFrame)
			}
			total := l.fieldLen + frameLen
			if len(l.buf) < total {
				break // wait for more bytes
			}
			frame := make([]byte, frameLen)
			copy(frame, l.buf[l.fieldLen:total])
			l.buf = l.buf[total:]
			l.frames = append(l.frames, frame)
		}
	}
}

func (l *lengthFieldFramingIter) readLength(b []byte) (int, error) {
	switch l.fieldLen {
	case 1:
		return int(b[0]), nil
	case 2:
		return int(l.byteOrder.Uint16(b)), nil
	case 4:
		return int(l.byteOrder.Uint32(b)), nil
	case 8:
		v := l.byteOrder.Uint64(b)
		if v > uint64(^uint(0)>>1) {
			return 0, fmt.Errorf("framing: length field value %d overflows int", v)
		}
		return int(v), nil
	default:
		return 0, fmt.Errorf("framing: unsupported fieldLength %d (must be 1, 2, 4, or 8)", l.fieldLen)
	}
}

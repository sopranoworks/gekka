/*
 * actor/delivery/proto.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package delivery

import "fmt"

// ---------------------------------------------------------------------------
// Proto2 wire-format constants
// ---------------------------------------------------------------------------

const (
	protoVarint = uint64(0)
	protoBytes  = uint64(2)
)

// ---------------------------------------------------------------------------
// Varint / bytes primitives
// ---------------------------------------------------------------------------

func appendTag(b []byte, fieldNum, wireType uint64) []byte {
	return appendVarint(b, (fieldNum<<3)|wireType)
}

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendBytes(b, data []byte) []byte {
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

func appendString(b []byte, s string) []byte {
	return appendBytes(b, []byte(s))
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

// consumeVarint reads a varint from data; returns (value, bytesConsumed).
// Returns (0, 0) on truncation or overflow.
func consumeVarint(data []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if i == 10 {
			return 0, 0
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, 0
			}
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

// consumeBytes reads a length-delimited field; returns (value, bytesConsumed).
// Returns (nil, 0) on truncation.
func consumeBytes(data []byte) ([]byte, int) {
	length, n := consumeVarint(data)
	if n == 0 {
		return nil, 0
	}
	end := n + int(length)
	if end > len(data) {
		return nil, 0
	}
	return data[n:end], end
}

// skipField skips a field of the given wire type.
func skipField(data []byte, wireType uint64) (int, error) {
	switch wireType {
	case protoVarint:
		_, n := consumeVarint(data)
		if n == 0 {
			return 0, fmt.Errorf("delivery: skipField varint truncated")
		}
		return n, nil
	case 1: // 64-bit
		if len(data) < 8 {
			return 0, fmt.Errorf("delivery: skipField 64-bit truncated")
		}
		return 8, nil
	case protoBytes:
		_, n := consumeBytes(data)
		if n == 0 {
			return 0, fmt.Errorf("delivery: skipField bytes truncated")
		}
		return n, nil
	case 5: // 32-bit
		if len(data) < 4 {
			return 0, fmt.Errorf("delivery: skipField 32-bit truncated")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("delivery: skipField unknown wiretype %d", wireType)
	}
}

// ---------------------------------------------------------------------------
// Payload proto encoding
//
// message Payload {
//   required bytes  enclosedMessage = 1;
//   required int32  serializerId    = 2;
//   optional bytes  messageManifest = 4;
// }
// ---------------------------------------------------------------------------

func encodePayload(p Payload) []byte {
	var b []byte
	b = appendTag(b, 1, protoBytes)
	b = appendBytes(b, p.EnclosedMessage)
	b = appendTag(b, 2, protoVarint)
	b = appendVarint(b, uint64(int32(p.SerializerID)))
	if p.Manifest != "" {
		b = appendTag(b, 4, protoBytes)
		b = appendBytes(b, []byte(p.Manifest))
	}
	return b
}

func decodePayload(data []byte) (Payload, error) {
	var p Payload
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return p, fmt.Errorf("delivery: Payload truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // enclosedMessage
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return p, fmt.Errorf("delivery: Payload.enclosedMessage truncated")
			}
			p.EnclosedMessage = append([]byte(nil), v...)
			i += n2
		case 2: // serializerId (varint)
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return p, fmt.Errorf("delivery: Payload.serializerId truncated")
			}
			p.SerializerID = int32(v)
			i += n2
		case 4: // messageManifest
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return p, fmt.Errorf("delivery: Payload.messageManifest truncated")
			}
			p.Manifest = string(v)
			i += n2
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return p, err
			}
			i += n2
		}
	}
	return p, nil
}

// ---------------------------------------------------------------------------
// SequencedMessage proto encoding  (manifest "a")
//
// message SequencedMessage {
//   required string producerId           = 1;
//   required int64  seqNr                = 2;
//   required bool   first                = 3;
//   required bool   ack                  = 4;
//   required string producerControllerRef = 5;
//   required Payload message             = 6;
//   optional bool   firstChunk           = 7;
//   optional bool   lastChunk            = 8;
// }
// ---------------------------------------------------------------------------

func EncodeSequencedMessage(m *SequencedMessage) []byte {
	var b []byte
	b = appendTag(b, 1, protoBytes)
	b = appendString(b, m.ProducerID)
	b = appendTag(b, 2, protoVarint)
	b = appendVarint(b, uint64(m.SeqNr))
	b = appendTag(b, 3, protoVarint)
	b = appendBool(b, m.First)
	b = appendTag(b, 4, protoVarint)
	b = appendBool(b, m.Ack)
	b = appendTag(b, 5, protoBytes)
	b = appendString(b, m.ProducerRef)

	payload := encodePayload(m.Message)
	b = appendTag(b, 6, protoBytes)
	b = appendBytes(b, payload)

	if m.HasChunk {
		b = appendTag(b, 7, protoVarint)
		b = appendBool(b, m.FirstChunk)
		b = appendTag(b, 8, protoVarint)
		b = appendBool(b, m.LastChunk)
	}
	return b
}

func DecodeSequencedMessage(data []byte) (*SequencedMessage, error) {
	var m SequencedMessage
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, fmt.Errorf("delivery: SequencedMessage truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1: // producerId
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.producerId truncated")
			}
			m.ProducerID = string(v)
			i += n2
		case 2: // seqNr (int64 varint)
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.seqNr truncated")
			}
			m.SeqNr = int64(v)
			i += n2
		case 3: // first
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.first truncated")
			}
			m.First = v != 0
			i += n2
		case 4: // ack
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.ack truncated")
			}
			m.Ack = v != 0
			i += n2
		case 5: // producerControllerRef
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.producerControllerRef truncated")
			}
			m.ProducerRef = string(v)
			i += n2
		case 6: // message (Payload)
			raw, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.message truncated")
			}
			p, err := decodePayload(raw)
			if err != nil {
				return nil, err
			}
			m.Message = p
			i += n2
		case 7: // firstChunk (optional)
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.firstChunk truncated")
			}
			m.HasChunk = true
			m.FirstChunk = v != 0
			i += n2
		case 8: // lastChunk (optional)
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: SequencedMessage.lastChunk truncated")
			}
			m.LastChunk = v != 0
			i += n2
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return nil, err
			}
			i += n2
		}
	}
	return &m, nil
}

// ---------------------------------------------------------------------------
// Ack proto encoding  (manifest "b")
//
// message Ack {
//   required int64 confirmedSeqNr = 1;
// }
// ---------------------------------------------------------------------------

func EncodeAck(m *AckMsg) []byte {
	var b []byte
	b = appendTag(b, 1, protoVarint)
	b = appendVarint(b, uint64(m.ConfirmedSeqNr))
	return b
}

func DecodeAck(data []byte) (*AckMsg, error) {
	var m AckMsg
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, fmt.Errorf("delivery: Ack truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 {
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Ack.confirmedSeqNr truncated")
			}
			m.ConfirmedSeqNr = int64(v)
			i += n2
		} else {
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return nil, err
			}
			i += n2
		}
	}
	return &m, nil
}

// ---------------------------------------------------------------------------
// Request proto encoding  (manifest "c")
//
// message Request {
//   required int64 confirmedSeqNr   = 1;
//   required int64 requestUpToSeqNr = 2;
//   required bool  supportResend    = 3;
//   required bool  viaTimeout       = 4;
// }
// ---------------------------------------------------------------------------

func EncodeRequest(m *Request) []byte {
	var b []byte
	b = appendTag(b, 1, protoVarint)
	b = appendVarint(b, uint64(m.ConfirmedSeqNr))
	b = appendTag(b, 2, protoVarint)
	b = appendVarint(b, uint64(m.RequestUpToSeqNr))
	b = appendTag(b, 3, protoVarint)
	b = appendBool(b, m.SupportResend)
	b = appendTag(b, 4, protoVarint)
	b = appendBool(b, m.ViaTimeout)
	return b
}

func DecodeRequest(data []byte) (*Request, error) {
	var m Request
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, fmt.Errorf("delivery: Request truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		switch fieldNum {
		case 1:
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Request.confirmedSeqNr truncated")
			}
			m.ConfirmedSeqNr = int64(v)
			i += n2
		case 2:
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Request.requestUpToSeqNr truncated")
			}
			m.RequestUpToSeqNr = int64(v)
			i += n2
		case 3:
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Request.supportResend truncated")
			}
			m.SupportResend = v != 0
			i += n2
		case 4:
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Request.viaTimeout truncated")
			}
			m.ViaTimeout = v != 0
			i += n2
		default:
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return nil, err
			}
			i += n2
		}
	}
	return &m, nil
}

// ---------------------------------------------------------------------------
// Resend proto encoding  (manifest "d")
//
// message Resend {
//   required int64 fromSeqNr = 1;
// }
// ---------------------------------------------------------------------------

func EncodeResend(m *Resend) []byte {
	var b []byte
	b = appendTag(b, 1, protoVarint)
	b = appendVarint(b, uint64(m.FromSeqNr))
	return b
}

func DecodeResend(data []byte) (*Resend, error) {
	var m Resend
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, fmt.Errorf("delivery: Resend truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 {
			v, n2 := consumeVarint(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: Resend.fromSeqNr truncated")
			}
			m.FromSeqNr = int64(v)
			i += n2
		} else {
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return nil, err
			}
			i += n2
		}
	}
	return &m, nil
}

// ---------------------------------------------------------------------------
// RegisterConsumer proto encoding  (manifest "e")
//
// message RegisterConsumer {
//   required string consumerControllerRef = 1;
// }
// ---------------------------------------------------------------------------

func EncodeRegisterConsumer(m *RegisterConsumer) []byte {
	var b []byte
	b = appendTag(b, 1, protoBytes)
	b = appendString(b, m.ConsumerControllerRef)
	return b
}

func DecodeRegisterConsumer(data []byte) (*RegisterConsumer, error) {
	var m RegisterConsumer
	i := 0
	for i < len(data) {
		tag, n := consumeVarint(data[i:])
		if n == 0 {
			return nil, fmt.Errorf("delivery: RegisterConsumer truncated tag at %d", i)
		}
		i += n
		fieldNum := tag >> 3
		wireType := tag & 7
		if fieldNum == 1 {
			v, n2 := consumeBytes(data[i:])
			if n2 == 0 {
				return nil, fmt.Errorf("delivery: RegisterConsumer.consumerControllerRef truncated")
			}
			m.ConsumerControllerRef = string(v)
			i += n2
		} else {
			n2, err := skipField(data[i:], wireType)
			if err != nil {
				return nil, err
			}
			i += n2
		}
	}
	return &m, nil
}

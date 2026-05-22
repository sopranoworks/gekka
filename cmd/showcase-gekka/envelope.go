// cmd/showcase-gekka/envelope.go — Go-side message types for the showcase.
//
// Each type mirrors a Scala case class in
// test/showcase/scala/src/main/scala/com/gekka/showcase/MessageTypes.scala
// and embeds jacksoncbor.JVMClassManifest so the gekka JacksonCborSerializer
// can emit the correct Pekko-side manifest on outbound and resolve inbound
// manifests back to the right Go type. CBOR tags use Scala field names
// (lowerCamelCase) to match what Pekko's Jackson emits.
//
// Polymorphic envelope payloads dispatch via the `PayloadKind` discriminator
// (CoercePayload below); Pekko's Jackson default emits no @class tag for
// AnyRef slots, so the dispatch is application-level, not wire-level.
//
// SPDX-License-Identifier: MIT
package main

import (
	"fmt"

	jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"
)

// JVM class identities — must match the Scala-side fully-qualified names.
const (
	JVMEchoEnvelope       = "com.gekka.showcase.EchoEnvelope"
	JVMAskEnvelope        = "com.gekka.showcase.AskEnvelope"
	JVMShowcaseEchoCustom = "com.gekka.showcase.ShowcaseEchoCustom"
	JVMSystemMessagePing  = "com.gekka.showcase.SystemMessagePing"
	JVMPing               = "com.gekka.showcase.Ping"
	JVMPong               = "com.gekka.showcase.Pong"
)

// EchoEnvelope is the FT1 message. Payload is interface{} because Pekko
// emits the AnyRef slot without a type tag — receivers coerce via
// PayloadKind in CoercePayload.
type EchoEnvelope struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64       `cbor:"seqNo"`
	Originator             string      `cbor:"originator"`
	Direction              string      `cbor:"direction"`
	PayloadKind            string      `cbor:"payloadKind"`
	Payload                interface{} `cbor:"payload"`
}

// NewEchoEnvelope constructs an EchoEnvelope with the JVM manifest pre-set.
func NewEchoEnvelope(seq int64, originator, direction, kind string, payload interface{}) *EchoEnvelope {
	return &EchoEnvelope{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMEchoEnvelope},
		SeqNo:            seq,
		Originator:       originator,
		Direction:        direction,
		PayloadKind:      kind,
		Payload:          payload,
	}
}

// AskEnvelope is the FT2 message. Same shape as EchoEnvelope; the actor
// path differs.
type AskEnvelope struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64       `cbor:"seqNo"`
	Originator             string      `cbor:"originator"`
	Direction              string      `cbor:"direction"`
	PayloadKind            string      `cbor:"payloadKind"`
	Payload                interface{} `cbor:"payload"`
}

func NewAskEnvelope(seq int64, originator, direction, kind string, payload interface{}) *AskEnvelope {
	return &AskEnvelope{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMAskEnvelope},
		SeqNo:            seq,
		Originator:       originator,
		Direction:        direction,
		PayloadKind:      kind,
		Payload:          payload,
	}
}

// ShowcaseEchoCustom is the "custom" payload kind — a Scala case class with
// a Vector[Byte] field that arrives as a CBOR array of small ints.
type ShowcaseEchoCustom struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64         `cbor:"seqNo"`
	Originator             string        `cbor:"originator"`
	Payload                []interface{} `cbor:"payload"` // []int64 from CBOR array of small uints
}

func NewShowcaseEchoCustom(seq int64, originator string, bytes []byte) *ShowcaseEchoCustom {
	payload := make([]interface{}, len(bytes))
	for i, b := range bytes {
		payload[i] = int64(b)
	}
	return &ShowcaseEchoCustom{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMShowcaseEchoCustom},
		SeqNo:            seq,
		Originator:       originator,
		Payload:          payload,
	}
}

// SystemMessagePing is the "system" payload kind — a real wire type
// replacing the Phase 1 "system-msg-stub" String stand-in.
type SystemMessagePing struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

func NewSystemMessagePing(seq int64, origin string) *SystemMessagePing {
	return &SystemMessagePing{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMSystemMessagePing},
		SeqNo:            seq,
		Origin:           origin,
	}
}

// Ping is the FT4 singleton-client request.
type Ping struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

func NewPing(seq int64, origin string) *Ping {
	return &Ping{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMPing},
		SeqNo:            seq,
		Origin:           origin,
	}
}

// Pong is the FT4 singleton response.
type Pong struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

func NewPong(seq int64, origin string) *Pong {
	return &Pong{
		JVMClassManifest: jcbor.JVMClassManifest{Class: JVMPong},
		SeqNo:            seq,
		Origin:           origin,
	}
}

// CoercePayload converts a generic decoded payload (map[interface{}]interface{}
// for nested objects, primitives for scalars) into the typed Go value
// indicated by the envelope's PayloadKind. Returns a structured error for
// unknown discriminators — never a panic, never a silent fallback.
//
// Mapping (must stay in sync with TellSenderActor.scala:50-55 and
// AskSenderActor.scala:43-48):
//   - "string" → string
//   - "long"   → int64
//   - "system" → *SystemMessagePing
//   - "custom" → *ShowcaseEchoCustom
func CoercePayload(kind string, payload interface{}) (interface{}, error) {
	switch kind {
	case "string":
		s, ok := payload.(string)
		if !ok {
			return nil, fmt.Errorf("envelope: payloadKind=string but payload is %T", payload)
		}
		return s, nil
	case "long":
		switch v := payload.(type) {
		case int64:
			return v, nil
		case uint64:
			return int64(v), nil
		default:
			return nil, fmt.Errorf("envelope: payloadKind=long but payload is %T", payload)
		}
	case "system":
		m, ok := payload.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("envelope: payloadKind=system but payload is %T (want map[interface{}]interface{})", payload)
		}
		seq, _ := m["seqNo"].(int64)
		origin, _ := m["origin"].(string)
		return NewSystemMessagePing(seq, origin), nil
	case "custom":
		m, ok := payload.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("envelope: payloadKind=custom but payload is %T (want map[interface{}]interface{})", payload)
		}
		seq, _ := m["seqNo"].(int64)
		originator, _ := m["originator"].(string)
		bytesArr, _ := m["payload"].([]interface{})
		bytes := make([]byte, 0, len(bytesArr))
		for _, v := range bytesArr {
			switch x := v.(type) {
			case int64:
				bytes = append(bytes, byte(x&0xff))
			case uint64:
				bytes = append(bytes, byte(x&0xff))
			}
		}
		return NewShowcaseEchoCustom(seq, originator, bytes), nil
	default:
		return nil, fmt.Errorf("envelope: unknown payloadKind %q (registered: string, long, system, custom)", kind)
	}
}

// RegisterShowcaseTypes registers all six showcase Go types with the given
// JacksonCborSerializer so it can decode any Scala-emitted ShowcaseMessage
// and emit any Go-side ShowcaseMessage with the correct manifest.
func RegisterShowcaseTypes(s *jcbor.Serializer) {
	s.Register(NewEchoEnvelope(0, "", "", "", nil))
	s.Register(NewAskEnvelope(0, "", "", "", nil))
	s.Register(NewShowcaseEchoCustom(0, "", nil))
	s.Register(NewSystemMessagePing(0, ""))
	s.Register(NewPing(0, ""))
	s.Register(NewPong(0, ""))
}

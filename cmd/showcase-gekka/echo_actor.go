// cmd/showcase-gekka/echo_actor.go — Gekka-side EchoActor for FT1 of the
// gekka_showcase_test plan.
//
// EchoActor receives a SEND-direction EchoEnvelope from a peer (Scala or Go)
// and replies with the same envelope flipped to REPLY direction. The reply
// is delivered to whoever the message Sender() resolves to — i.e. the
// originating TellSender on the remote node.
//
// SPDX-License-Identifier: MIT
package main

import (
	"fmt"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/logger"
)

// EchoEnvelope mirrors the Scala case class.
//
// NOTE: Scala binds ShowcaseMessage to jackson-cbor (serializer ID ~33). The
// Gekka SerializationRegistry does not yet know that serializer, so when a
// Scala EchoEnvelope arrives at this actor over Artery it is delivered as a
// *gekka.IncomingMessage carrying the raw CBOR bytes. The cross-language
// codec wiring is a plan-level concern tracked separately; this actor
// handles both the typed-local case (*EchoEnvelope) and the raw-remote case
// (*gekka.IncomingMessage) so it cannot silently drop messages.
type EchoEnvelope struct {
	SeqNo       int64       `json:"seqNo"`
	Originator  string      `json:"originator"`
	Direction   string      `json:"direction"`
	PayloadKind string      `json:"payloadKind"`
	Payload     interface{} `json:"payload"`
}

// EchoActor receives an EchoEnvelope, flips its Direction to REPLY, and tells
// it back to the sender. Mirrors the Scala-side EchoActor.
type EchoActor struct {
	actor.BaseActor
}

// Receive handles incoming messages. The typed *EchoEnvelope branch covers
// local in-process testing and the future case where a CBOR-compatible
// serializer is registered; the *gekka.IncomingMessage branch documents the
// raw-bytes path so an architectural failure surfaces as a structured ERROR
// log rather than a silent drop.
func (a *EchoActor) Receive(msg any) {
	switch m := msg.(type) {
	case *EchoEnvelope:
		if m.Direction != "SEND" {
			logger.Default().Error("EchoActor: non-SEND envelope",
				"direction", m.Direction,
				"seq", m.SeqNo,
				"originator", m.Originator)
			return
		}
		reply := *m
		reply.Direction = "REPLY"
		if s := a.Sender(); s != nil && s.Path() != "" {
			s.Tell(&reply, a.Self())
			return
		}
		logger.Default().Error("EchoActor: no sender on SEND envelope",
			"seq", m.SeqNo,
			"originator", m.Originator)

	case *gekka.IncomingMessage:
		// Raw remote delivery — the deserializer for this serializer ID is
		// not registered on the Gekka side. Surface a structured error so
		// the smoke test catches it; do not crash the actor.
		logger.Default().Error("EchoActor: unsupported remote envelope",
			"serializerId", m.SerializerId,
			"manifest", m.Manifest,
			"payloadLen", len(m.Payload),
			"recipient", m.RecipientPath)

	default:
		logger.Default().Error("EchoActor: unexpected message",
			"type", fmtType(msg))
	}
}

// fmtType returns a printable type name for a message, used in error logs.
func fmtType(v any) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T", v)
}

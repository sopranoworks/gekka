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

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/logger"
)

// EchoActor receives an EchoEnvelope, flips its Direction to REPLY, and tells
// it back to the sender. Mirrors the Scala-side EchoActor.
//
// EchoEnvelope is defined in envelope.go; the Phase 3 JacksonCborSerializer
// + showcase-gekka wiring decodes Scala-emitted jackson-cbor frames directly
// into *EchoEnvelope. Polymorphic payloads (the AnyRef slot) arrive as
// loose map[interface{}]interface{} or scalar; CoercePayload turns them
// into typed Go values when the actor needs them.
type EchoActor struct {
	actor.BaseActor
}

// Receive handles incoming EchoEnvelope messages.
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

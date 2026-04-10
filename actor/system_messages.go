/*
 * system_messages.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "fmt"

// ── PoisonPill / Kill ──────────────────────────────────────────────────────

// PoisonPill is a system message that causes the receiving actor to stop
// gracefully after processing the current message. PostStop is called.
type PoisonPill struct{}

// Kill is a system message that causes the receiving actor to throw an
// ActorKilledException, triggering its supervisor strategy.
type Kill struct{}

// ActorKilledException is the error raised when an actor receives a Kill message.
type ActorKilledException struct {
	Actor Ref
}

func (e *ActorKilledException) Error() string {
	path := "<nil>"
	if e.Actor != nil {
		path = e.Actor.Path()
	}
	return fmt.Sprintf("ActorKilledException: actor %s was killed", path)
}

// ── Identify / ActorIdentity ───────────────────────────────────────────────

// Identify is a system message that requests an actor to reveal its identity.
// When received by an actor, it replies with an ActorIdentity message to the
// sender. MessageID is an arbitrary correlation token chosen by the caller.
type Identify struct {
	MessageID any
}

// ActorIdentity is the reply to an Identify message. Ref is non-nil if the
// actor exists; nil otherwise. MessageID matches the Identify request.
type ActorIdentity struct {
	MessageID any
	Ref       Ref
}

// ── ReceiveTimeout ─────────────────────────────────────────────────────────

// ControlMessage is a marker interface for messages that should be processed
// with priority over normal messages when using a ControlAwareMailbox.
type ControlMessage interface {
	IsControlMessage()
}

// ReceiveTimeout is a system message delivered to a classic actor when no
// messages have been received within the configured timeout duration.
type ReceiveTimeout struct{}

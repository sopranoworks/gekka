/*
 * event_stream.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"reflect"
)

// DeadLetter is published to the EventStream whenever a message cannot be
// delivered to its intended recipient. This mirrors Pekko's
// org.apache.pekko.actor.DeadLetter.
//
// Actors that want to monitor undeliverable messages subscribe to
// reflect.TypeOf(actor.DeadLetter{}) on the system EventStream:
//
//	sys.EventStream().Subscribe(self, reflect.TypeOf(actor.DeadLetter{}))
//
// Cause records the reason for the dead-letter delivery ("mailbox-closed",
// "mailbox-full", or "not-found").
type DeadLetter struct {
	Message   any    // the original message that could not be delivered
	Sender    Ref    // sender reference; nil when no sender was specified
	Recipient Ref    // the target actor reference
	Cause     string // "mailbox-closed" | "mailbox-full" | "not-found"
}

// EventStream is a system-wide, type-based publish/subscribe bus that mirrors
// Pekko's org.apache.pekko.event.EventStream.
//
// In Pekko, EventStream extends LoggingBus and mixes in SubchannelClassification.
// In gekka, EventStream composes LookupClassification to provide the same
// type-matching semantics (exact type match and interface satisfaction) while
// adding UnsubscribeAll for lifecycle cleanup.
//
// EventStream is safe for concurrent use from multiple goroutines.
//
// Typical usage inside an actor:
//
//	// Subscribe during PreStart:
//	sys.EventStream().Subscribe(a.Self(), reflect.TypeOf(MemberUp{}))
//
//	// Publish a system event:
//	sys.EventStream().Publish(MemberUp{Member: m})
//
//	// Unsubscribe before shutdown:
//	sys.EventStream().Unsubscribe(a.Self(), reflect.TypeOf(MemberUp{}))
type EventStream struct {
	LookupClassification
}

// NewEventStream returns a new, empty EventStream.
func NewEventStream() *EventStream {
	return &EventStream{
		LookupClassification: *NewLookupClassification(),
	}
}

// UnsubscribeAll removes subscriber from every topic it has subscribed to.
// Typically called in PostStop to avoid dead-letter noise after an actor stops.
func (es *EventStream) UnsubscribeAll(subscriber Ref) {
	es.mu.Lock()
	defer es.mu.Unlock()
	for topic, list := range es.subscribers {
		filtered := list[:0]
		for _, ref := range list {
			if ref != subscriber {
				filtered = append(filtered, ref)
			}
		}
		es.subscribers[topic] = filtered
	}
}

// Subscribe registers subscriber to receive events of the given topic type.
// Duplicate subscriptions for the same (subscriber, topic) pair are silently
// ignored.
func (es *EventStream) Subscribe(subscriber Ref, topic reflect.Type) {
	es.LookupClassification.Subscribe(subscriber, topic)
}

// Unsubscribe removes subscriber from all deliveries for topic.
// It is a no-op when the subscription does not exist.
func (es *EventStream) Unsubscribe(subscriber Ref, topic reflect.Type) {
	es.LookupClassification.Unsubscribe(subscriber, topic)
}

// Publish delivers event to every subscriber whose topic type matches the
// dynamic type of event. A subscriber registered for an interface type T
// receives the event when reflect.TypeOf(event) implements T; a subscriber
// registered for a concrete type receives it only on an exact match.
//
// Delivery is synchronous: Publish returns once Tell has been called on every
// matching subscriber. Subscribers receive the event as the raw value (not
// wrapped).
func (es *EventStream) Publish(event any) {
	es.LookupClassification.Publish(event)
}

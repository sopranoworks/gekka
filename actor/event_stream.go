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
	"sync"
)

// EventStream is a system-wide, type-based publish/subscribe bus that mirrors
// Pekko's org.apache.pekko.event.EventStream.
//
// Topics are identified by Go reflect.Type values. Subscribing to type T
// means the subscriber receives all published events whose dynamic type is
// exactly T or implements T (when T is an interface). Publishing an event
// delivers it to every subscriber whose topic matches.
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
	mu          sync.RWMutex
	subscribers map[reflect.Type][]Ref
}

// NewEventStream returns a new, empty EventStream.
func NewEventStream() *EventStream {
	return &EventStream{
		subscribers: make(map[reflect.Type][]Ref),
	}
}

// Subscribe registers subscriber to receive events of the given topic type.
// Duplicate subscriptions for the same (subscriber, topic) pair are silently
// ignored.
func (es *EventStream) Subscribe(subscriber Ref, topic reflect.Type) {
	es.mu.Lock()
	defer es.mu.Unlock()
	for _, existing := range es.subscribers[topic] {
		if existing == subscriber {
			return
		}
	}
	es.subscribers[topic] = append(es.subscribers[topic], subscriber)
}

// Unsubscribe removes subscriber from all deliveries for topic.
// It is a no-op when the subscription does not exist.
func (es *EventStream) Unsubscribe(subscriber Ref, topic reflect.Type) {
	es.mu.Lock()
	defer es.mu.Unlock()
	list := es.subscribers[topic]
	for i, ref := range list {
		if ref == subscriber {
			es.subscribers[topic] = append(list[:i], list[i+1:]...)
			return
		}
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

// Publish delivers event to every subscriber whose topic type matches the
// dynamic type of event. A subscriber registered for an interface type T
// receives the event when reflect.TypeOf(event) implements T; a subscriber
// registered for a concrete type receives it only on an exact match.
//
// Delivery is synchronous: Publish returns once Tell has been called on every
// matching subscriber. Subscribers receive the event as the raw value (not
// wrapped).
func (es *EventStream) Publish(event any) {
	if event == nil {
		return
	}
	eventType := reflect.TypeOf(event)

	es.mu.RLock()
	// Collect matching subscribers without holding the lock during delivery.
	var targets []Ref
	for topic, subs := range es.subscribers {
		if matches(eventType, topic) {
			targets = append(targets, subs...)
		}
	}
	es.mu.RUnlock()

	for _, sub := range targets {
		sub.Tell(event)
	}
}

// matches reports whether eventType satisfies topic.
//   - If topic is an interface, eventType must implement it.
//   - Otherwise, eventType must equal topic exactly (or be assignable to topic
//     for pointer/value variants of the same struct).
func matches(eventType, topic reflect.Type) bool {
	if topic.Kind() == reflect.Interface {
		return eventType.Implements(topic)
	}
	return eventType == topic || eventType.AssignableTo(topic)
}

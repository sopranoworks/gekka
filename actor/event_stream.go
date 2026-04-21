/*
 * event_stream.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"log"
	"reflect"
	"sync"
	"sync/atomic"
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
	funcMu   sync.RWMutex
	funcSubs map[reflect.Type][]func(event any)
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

// SubscribeFunc registers a callback function for events of the given type.
// This is used internally for system-level subscribers that don't have an actor Ref.
func (es *EventStream) SubscribeFunc(topic reflect.Type, fn func(event any)) {
	es.funcMu.Lock()
	defer es.funcMu.Unlock()
	if es.funcSubs == nil {
		es.funcSubs = make(map[reflect.Type][]func(event any))
	}
	es.funcSubs[topic] = append(es.funcSubs[topic], fn)
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

	// Also invoke function-based subscribers.
	if event == nil {
		return
	}
	es.funcMu.RLock()
	fns := es.funcSubs[reflect.TypeOf(event)]
	es.funcMu.RUnlock()
	for _, fn := range fns {
		fn(event)
	}
}

// DeadLetterLogger is an internal subscriber that logs DeadLetter events
// up to a configurable limit, matching Pekko's pekko.log-dead-letters behavior.
type DeadLetterLogger struct {
	mu             sync.Mutex
	remaining      int   // positive = countdown, 0 = exhausted, negative = unlimited
	suppressDuring bool  // suppress during shutdown
	isShuttingDown *int32 // pointer to system's shuttingDown flag (atomic)
	subscribed     bool
}

// Subscribed reports whether this logger is actively subscribed to the EventStream.
func (dl *DeadLetterLogger) Subscribed() bool {
	return dl.subscribed
}

// NewDeadLetterLogger creates a logger that logs up to `limit` dead letters.
// limit: 0=off, positive=countdown, negative=unlimited.
func NewDeadLetterLogger(es *EventStream, limit int, suppressDuringShutdown bool, shuttingDown *int32) *DeadLetterLogger {
	dl := &DeadLetterLogger{
		remaining:      limit,
		suppressDuring: suppressDuringShutdown,
		isShuttingDown: shuttingDown,
	}
	if limit != 0 {
		es.SubscribeFunc(reflect.TypeOf(DeadLetter{}), dl.handle)
		dl.subscribed = true
	}
	return dl
}

func (dl *DeadLetterLogger) handle(event any) {
	d, ok := event.(DeadLetter)
	if !ok {
		return
	}
	// Suppress during shutdown if configured
	if dl.suppressDuring && atomic.LoadInt32(dl.isShuttingDown) != 0 {
		return
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()
	if dl.remaining == 0 {
		return
	}
	recipientPath := ""
	if d.Recipient != nil {
		recipientPath = d.Recipient.Path()
	}
	log.Printf("Dead letter [%s] from %s to %s: %v",
		d.Cause, deadLetterRefPath(d.Sender), recipientPath, d.Message)
	if dl.remaining > 0 {
		dl.remaining--
		if dl.remaining == 0 {
			log.Printf("Dead letter logging limit reached. No further dead letters will be logged. " +
				"To change this limit, configure pekko.log-dead-letters.")
		}
	}
}

func deadLetterRefPath(r Ref) string {
	if r == nil {
		return "[no sender]"
	}
	return r.Path()
}

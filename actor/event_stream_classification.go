/*
 * event_stream_classification.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"reflect"
	"strings"
	"sync"
)

// LookupClassification is a type-based event classification bus.
// Subscribers register for a specific reflect.Type and receive events
// whose dynamic type matches exactly or implements the subscribed type
// (when the topic is an interface).
//
// This mirrors Pekko's LookupClassification trait for EventBus.
type LookupClassification struct {
	mu          sync.RWMutex
	subscribers map[reflect.Type][]Ref
}

// NewLookupClassification returns a new, empty LookupClassification bus.
func NewLookupClassification() *LookupClassification {
	return &LookupClassification{
		subscribers: make(map[reflect.Type][]Ref),
	}
}

// Subscribe registers subscriber to receive events of the given topic type.
// Duplicate subscriptions for the same (subscriber, topic) pair are ignored.
func (lc *LookupClassification) Subscribe(subscriber Ref, topic reflect.Type) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, existing := range lc.subscribers[topic] {
		if existing == subscriber {
			return
		}
	}
	lc.subscribers[topic] = append(lc.subscribers[topic], subscriber)
}

// Unsubscribe removes subscriber from deliveries for topic.
// It is a no-op when the subscription does not exist.
func (lc *LookupClassification) Unsubscribe(subscriber Ref, topic reflect.Type) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	list := lc.subscribers[topic]
	for i, ref := range list {
		if ref == subscriber {
			lc.subscribers[topic] = append(list[:i], list[i+1:]...)
			return
		}
	}
}

// Publish delivers event to every subscriber whose topic type matches the
// dynamic type of event. Uses the same matching semantics as EventStream:
// exact type match or interface satisfaction.
func (lc *LookupClassification) Publish(event any) {
	if event == nil {
		return
	}
	eventType := reflect.TypeOf(event)

	lc.mu.RLock()
	var targets []Ref
	for topic, subs := range lc.subscribers {
		if matches(eventType, topic) {
			targets = append(targets, subs...)
		}
	}
	lc.mu.RUnlock()

	for _, sub := range targets {
		sub.Tell(event)
	}
}

// SubchannelClassification is a hierarchical channel-based event bus.
// Subscribers register for a dotted channel path (e.g. "app.db") and receive
// events published to that channel or any sub-channel beneath it. For example,
// a subscriber to "app" receives events published to "app", "app.db", and
// "app.db.query".
//
// This mirrors Pekko's SubchannelClassification trait for EventBus.
type SubchannelClassification struct {
	mu          sync.RWMutex
	subscribers map[string][]Ref
}

// NewSubchannelClassification returns a new, empty SubchannelClassification bus.
func NewSubchannelClassification() *SubchannelClassification {
	return &SubchannelClassification{
		subscribers: make(map[string][]Ref),
	}
}

// Subscribe registers subscriber to receive events on the given channel.
// Duplicate subscriptions for the same (subscriber, channel) pair are ignored.
func (sc *SubchannelClassification) Subscribe(subscriber Ref, channel string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, existing := range sc.subscribers[channel] {
		if existing == subscriber {
			return
		}
	}
	sc.subscribers[channel] = append(sc.subscribers[channel], subscriber)
}

// Unsubscribe removes subscriber from deliveries for channel.
// It is a no-op when the subscription does not exist.
func (sc *SubchannelClassification) Unsubscribe(subscriber Ref, channel string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	list := sc.subscribers[channel]
	for i, ref := range list {
		if ref == subscriber {
			sc.subscribers[channel] = append(list[:i], list[i+1:]...)
			return
		}
	}
}

// Publish delivers event to every subscriber whose channel is a prefix of
// (or equal to) the event's channel. The prefix matching uses "." as the
// hierarchy separator. For example, publishing to "app.db.query" delivers
// to subscribers of "app", "app.db", and "app.db.query", but not "ap" or
// "app.d".
func (sc *SubchannelClassification) Publish(channel string, event any) {
	sc.mu.RLock()
	var targets []Ref
	for subChannel, subs := range sc.subscribers {
		if channelMatches(subChannel, channel) {
			targets = append(targets, subs...)
		}
	}
	sc.mu.RUnlock()

	for _, sub := range targets {
		sub.Tell(event)
	}
}

// channelMatches reports whether subscription is a prefix of (or equal to)
// eventChannel using "." as the hierarchy separator.
func channelMatches(subscription, eventChannel string) bool {
	if subscription == eventChannel {
		return true
	}
	// subscription must be a proper prefix followed by "."
	return strings.HasPrefix(eventChannel, subscription+".")
}

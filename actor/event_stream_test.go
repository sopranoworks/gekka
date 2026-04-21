/*
 * event_stream_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor_test

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// ── Test helpers ─────────────────────────────────────────────────────────────

// capturingRef captures every message delivered via Tell.
type capturingRef struct {
	mu   sync.Mutex
	msgs []any
}

func (r *capturingRef) Tell(msg any, _ ...actor.Ref) {
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()
}

func (r *capturingRef) Path() string { return "/test/capture" }

func (r *capturingRef) received() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]any, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// ── Event types for tests ─────────────────────────────────────────────────────

type ConcreteEvent struct{ Value string }
type OtherEvent struct{ Value int }
type eventIface interface{ eventMarker() }
type ConcreteIfaceEvent struct{ Value string }

func (ConcreteIfaceEvent) eventMarker() {}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestEventStream_SubscribeAndPublish(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}

	es.Subscribe(sub, reflect.TypeOf(ConcreteEvent{}))
	es.Publish(ConcreteEvent{Value: "hello"})

	msgs := sub.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if v, ok := msgs[0].(ConcreteEvent); !ok || v.Value != "hello" {
		t.Errorf("unexpected message: %v", msgs[0])
	}
}

func TestEventStream_NoDeliveryWithoutSubscription(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}

	es.Publish(ConcreteEvent{Value: "missed"})

	if len(sub.received()) != 0 {
		t.Errorf("expected 0 messages without subscription, got %d", len(sub.received()))
	}
	_ = sub
}

func TestEventStream_UnsubscribeStopsDelivery(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}
	topic := reflect.TypeOf(ConcreteEvent{})

	es.Subscribe(sub, topic)
	es.Publish(ConcreteEvent{Value: "before"})
	es.Unsubscribe(sub, topic)
	es.Publish(ConcreteEvent{Value: "after"})

	msgs := sub.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message after unsubscribe, got %d", len(msgs))
	}
	if msgs[0].(ConcreteEvent).Value != "before" {
		t.Errorf("unexpected message: %v", msgs[0])
	}
}

func TestEventStream_UnsubscribeAll(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}

	es.Subscribe(sub, reflect.TypeOf(ConcreteEvent{}))
	es.Subscribe(sub, reflect.TypeOf(OtherEvent{}))
	es.UnsubscribeAll(sub)

	es.Publish(ConcreteEvent{Value: "c"})
	es.Publish(OtherEvent{Value: 42})

	if n := len(sub.received()); n != 0 {
		t.Errorf("expected 0 messages after UnsubscribeAll, got %d", n)
	}
}

func TestEventStream_MultipleSubscribers(t *testing.T) {
	es := actor.NewEventStream()
	s1, s2 := &capturingRef{}, &capturingRef{}
	topic := reflect.TypeOf(ConcreteEvent{})

	es.Subscribe(s1, topic)
	es.Subscribe(s2, topic)
	es.Publish(ConcreteEvent{Value: "broadcast"})

	for i, sub := range []*capturingRef{s1, s2} {
		if n := len(sub.received()); n != 1 {
			t.Errorf("subscriber %d: expected 1 message, got %d", i, n)
		}
	}
}

func TestEventStream_DuplicateSubscriptionIgnored(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}
	topic := reflect.TypeOf(ConcreteEvent{})

	es.Subscribe(sub, topic)
	es.Subscribe(sub, topic) // duplicate
	es.Publish(ConcreteEvent{Value: "once"})

	if n := len(sub.received()); n != 1 {
		t.Errorf("expected 1 delivery (duplicate subscription ignored), got %d", n)
	}
}

func TestEventStream_InterfaceTopicMatching(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}

	// Subscribe to an interface type: should receive ConcreteIfaceEvent.
	es.Subscribe(sub, reflect.TypeOf((*eventIface)(nil)).Elem())
	es.Publish(ConcreteIfaceEvent{Value: "via interface"})

	msgs := sub.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message via interface subscription, got %d", len(msgs))
	}
	if v, ok := msgs[0].(ConcreteIfaceEvent); !ok || v.Value != "via interface" {
		t.Errorf("unexpected message: %v", msgs[0])
	}
}

func TestEventStream_DifferentTypesIsolated(t *testing.T) {
	es := actor.NewEventStream()
	concSub := &capturingRef{}
	otherSub := &capturingRef{}

	es.Subscribe(concSub, reflect.TypeOf(ConcreteEvent{}))
	es.Subscribe(otherSub, reflect.TypeOf(OtherEvent{}))

	es.Publish(ConcreteEvent{Value: "c"})
	es.Publish(OtherEvent{Value: 99})

	if n := len(concSub.received()); n != 1 {
		t.Errorf("concSub: expected 1 message, got %d", n)
	}
	if n := len(otherSub.received()); n != 1 {
		t.Errorf("otherSub: expected 1 message, got %d", n)
	}
	// Ensure no cross-delivery
	for _, msg := range concSub.received() {
		if _, ok := msg.(OtherEvent); ok {
			t.Error("concSub received OtherEvent unexpectedly")
		}
	}
}

func TestEventStream_PublishNilIsNoOp(t *testing.T) {
	es := actor.NewEventStream()
	sub := &capturingRef{}
	es.Subscribe(sub, reflect.TypeOf(ConcreteEvent{}))
	es.Publish(nil) // must not panic
	if n := len(sub.received()); n != 0 {
		t.Errorf("nil publish should deliver nothing, got %d", n)
	}
}

func TestDeadLetterLogger_CountsDown(t *testing.T) {
	es := actor.NewEventStream()
	var shutDown int32
	_ = actor.NewDeadLetterLogger(es, 2, false, &shutDown)

	es.Publish(actor.DeadLetter{Message: "msg1", Cause: "not-found"})
	es.Publish(actor.DeadLetter{Message: "msg2", Cause: "not-found"})
	// Third should be suppressed (no crash, no log)
	es.Publish(actor.DeadLetter{Message: "msg3", Cause: "not-found"})
}

func TestDeadLetterLogger_Off(t *testing.T) {
	es := actor.NewEventStream()
	var shutDown int32
	dl := actor.NewDeadLetterLogger(es, 0, false, &shutDown)
	if dl.Subscribed() {
		t.Error("expected no subscription when limit=0")
	}
}

func TestDeadLetterLogger_SuppressDuringShutdown(t *testing.T) {
	es := actor.NewEventStream()
	var shutDown int32
	_ = actor.NewDeadLetterLogger(es, -1, true, &shutDown)

	atomic.StoreInt32(&shutDown, 1)
	// Should not panic or log
	es.Publish(actor.DeadLetter{Message: "msg", Cause: "mailbox-closed"})
}

func TestSubscribeFunc(t *testing.T) {
	es := actor.NewEventStream()
	var count int
	es.SubscribeFunc(reflect.TypeOf(ConcreteEvent{}), func(event any) {
		count++
	})
	es.Publish(ConcreteEvent{Value: "test"})
	es.Publish(ConcreteEvent{Value: "test2"})
	if count != 2 {
		t.Errorf("SubscribeFunc callback count = %d, want 2", count)
	}
}

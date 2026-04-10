/*
 * event_stream_classification_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"reflect"
	"testing"
)

// collectingRef is a simple Ref implementation that collects messages for assertions.
type collectingRef struct {
	path     string
	messages []any
}

func (r *collectingRef) Tell(msg any, sender ...Ref) { r.messages = append(r.messages, msg) }
func (r *collectingRef) Path() string                { return r.path }

func TestLookupClassification_TypeRouting(t *testing.T) {
	bus := NewLookupClassification()

	type Alpha struct{ Value int }
	type Beta struct{ Name string }

	subA := &collectingRef{path: "/alpha-subscriber"}
	subB := &collectingRef{path: "/beta-subscriber"}

	bus.Subscribe(subA, reflect.TypeOf(Alpha{}))
	bus.Subscribe(subB, reflect.TypeOf(Beta{}))

	bus.Publish(Alpha{Value: 42})
	bus.Publish(Beta{Name: "hello"})

	if len(subA.messages) != 1 {
		t.Fatalf("expected subA to receive 1 message, got %d", len(subA.messages))
	}
	if got, ok := subA.messages[0].(Alpha); !ok || got.Value != 42 {
		t.Fatalf("expected Alpha{42}, got %v", subA.messages[0])
	}

	if len(subB.messages) != 1 {
		t.Fatalf("expected subB to receive 1 message, got %d", len(subB.messages))
	}
	if got, ok := subB.messages[0].(Beta); !ok || got.Name != "hello" {
		t.Fatalf("expected Beta{hello}, got %v", subB.messages[0])
	}
}

func TestSubchannelClassification_HierarchicalMatching(t *testing.T) {
	bus := NewSubchannelClassification()

	parentSub := &collectingRef{path: "/parent"}
	childSub := &collectingRef{path: "/child"}

	bus.Subscribe(parentSub, "app")
	bus.Subscribe(childSub, "app.db")

	bus.Publish("app.db", "db-event")
	bus.Publish("app", "app-event")

	// parentSub subscribed to "app" should receive both "app.db" and "app" events
	if len(parentSub.messages) != 2 {
		t.Fatalf("expected parent to receive 2 messages, got %d: %v", len(parentSub.messages), parentSub.messages)
	}
	if parentSub.messages[0] != "db-event" {
		t.Fatalf("expected first parent message to be 'db-event', got %v", parentSub.messages[0])
	}
	if parentSub.messages[1] != "app-event" {
		t.Fatalf("expected second parent message to be 'app-event', got %v", parentSub.messages[1])
	}

	// childSub subscribed to "app.db" should receive only "app.db" events
	if len(childSub.messages) != 1 {
		t.Fatalf("expected child to receive 1 message, got %d: %v", len(childSub.messages), childSub.messages)
	}
	if childSub.messages[0] != "db-event" {
		t.Fatalf("expected child message to be 'db-event', got %v", childSub.messages[0])
	}
}

func TestLookupClassification_Unsubscribe(t *testing.T) {
	bus := NewLookupClassification()

	type Msg struct{ Val int }
	sub := &collectingRef{path: "/sub"}
	topic := reflect.TypeOf(Msg{})

	bus.Subscribe(sub, topic)
	bus.Publish(Msg{Val: 1})

	if len(sub.messages) != 1 {
		t.Fatalf("expected 1 message before unsubscribe, got %d", len(sub.messages))
	}

	bus.Unsubscribe(sub, topic)
	bus.Publish(Msg{Val: 2})

	if len(sub.messages) != 1 {
		t.Fatalf("expected still 1 message after unsubscribe, got %d", len(sub.messages))
	}
}

func TestSubchannelClassification_Unsubscribe(t *testing.T) {
	bus := NewSubchannelClassification()

	sub := &collectingRef{path: "/sub"}

	bus.Subscribe(sub, "app.db")
	bus.Publish("app.db.query", "event-1")

	if len(sub.messages) != 1 {
		t.Fatalf("expected 1 message before unsubscribe, got %d", len(sub.messages))
	}

	bus.Unsubscribe(sub, "app.db")
	bus.Publish("app.db.query", "event-2")

	if len(sub.messages) != 1 {
		t.Fatalf("expected still 1 message after unsubscribe, got %d", len(sub.messages))
	}
}

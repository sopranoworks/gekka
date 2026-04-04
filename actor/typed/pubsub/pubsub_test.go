/*
 * pubsub_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	typedpkg "github.com/sopranoworks/gekka/actor/typed"
	typedpubsub "github.com/sopranoworks/gekka/actor/typed/pubsub"
	"github.com/sopranoworks/gekka/cluster/pubsub"
)

// ─── mock Mediator ──────────────────────────────────────────────────────────

type mockMediator struct {
	mu           sync.Mutex
	published    []pubRecord
	subscribed   []subRecord
	unsubscribed []subRecord
}

type pubRecord struct {
	topic string
	msg   any
}

type subRecord struct {
	topic string
	group string
	path  string
}

func (m *mockMediator) Publish(_ context.Context, topic string, msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, pubRecord{topic, msg})
	return nil
}

func (m *mockMediator) Send(_ context.Context, _ string, _ any, _ bool) error { return nil }

func (m *mockMediator) Subscribe(_ context.Context, topic, group, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribed = append(m.subscribed, subRecord{topic, group, path})
	return nil
}

func (m *mockMediator) Unsubscribe(_ context.Context, topic, group, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribed = append(m.unsubscribed, subRecord{topic, group, path})
	return nil
}

// ─── path-only actor.Ref for tests ─────────────────────────────────────────

type pathRef struct{ path string }

func (r *pathRef) Path() string                      { return r.path }
func (r *pathRef) Tell(_ any, _ ...actor.Ref) {}

// ─── tests ──────────────────────────────────────────────────────────────────

// TestTopic_Publish verifies that a Publish[T] command is forwarded to the mediator.
func TestTopic_Publish(t *testing.T) {
	med := &mockMediator{}
	var topic typedpubsub.Topic[string]
	beh := topic.Behavior("events", med)

	// Invoke the behavior with nil context: handle() only calls ctx.Log() on error,
	// and the mock mediator never returns an error.
	beh(nil, typedpubsub.Publish[string]{Message: "hello"})

	med.mu.Lock()
	defer med.mu.Unlock()
	if len(med.published) != 1 {
		t.Fatalf("expected 1 Publish call, got %d", len(med.published))
	}
	r := med.published[0]
	if r.topic != "events" {
		t.Errorf("expected topic 'events', got %q", r.topic)
	}
	if r.msg != "hello" {
		t.Errorf("expected msg 'hello', got %v", r.msg)
	}
}

// TestTopic_Subscribe verifies that a Subscribe[T] command calls mediator.Subscribe.
func TestTopic_Subscribe(t *testing.T) {
	med := &mockMediator{}
	var topic typedpubsub.Topic[string]
	beh := topic.Behavior("chat", med)

	subRef := typedpkg.NewTypedActorRef[string](&pathRef{"/user/consumer"})
	beh(nil, typedpubsub.Subscribe[string]{Subscriber: subRef})

	med.mu.Lock()
	defer med.mu.Unlock()
	if len(med.subscribed) != 1 {
		t.Fatalf("expected 1 Subscribe call, got %d", len(med.subscribed))
	}
	s := med.subscribed[0]
	if s.topic != "chat" {
		t.Errorf("expected topic 'chat', got %q", s.topic)
	}
	if s.path != "/user/consumer" {
		t.Errorf("expected path '/user/consumer', got %q", s.path)
	}
}

// TestTopic_Unsubscribe verifies that an Unsubscribe[T] command calls mediator.Unsubscribe.
func TestTopic_Unsubscribe(t *testing.T) {
	med := &mockMediator{}
	var topic typedpubsub.Topic[string]
	beh := topic.Behavior("chat", med)

	subRef := typedpkg.NewTypedActorRef[string](&pathRef{"/user/consumer"})
	beh(nil, typedpubsub.Unsubscribe[string]{Subscriber: subRef})

	med.mu.Lock()
	defer med.mu.Unlock()
	if len(med.unsubscribed) != 1 {
		t.Fatalf("expected 1 Unsubscribe call, got %d", len(med.unsubscribed))
	}
	u := med.unsubscribed[0]
	if u.topic != "chat" {
		t.Errorf("expected topic 'chat', got %q", u.topic)
	}
	if u.path != "/user/consumer" {
		t.Errorf("expected path '/user/consumer', got %q", u.path)
	}
}

// TestTopic_UnknownCommandIgnored verifies that non-Command messages do not
// trigger any mediator call.
func TestTopic_UnknownCommandIgnored(t *testing.T) {
	med := &mockMediator{}
	var topic typedpubsub.Topic[string]
	beh := topic.Behavior("test", med)

	beh(nil, "not-a-command")
	beh(nil, 42)

	med.mu.Lock()
	defer med.mu.Unlock()
	total := len(med.published) + len(med.subscribed) + len(med.unsubscribed)
	if total != 0 {
		t.Fatalf("unknown messages must not trigger mediator calls, got %d calls", total)
	}
}

// TestTopic_EndToEnd verifies a full publish-and-receive cycle using LocalMediator.
func TestTopic_EndToEnd(t *testing.T) {
	localMed := pubsub.NewLocalMediator()
	ctx := context.Background()

	received := make(chan string, 8)
	const subPath = "/user/listener"
	localMed.RegisterReceiver(subPath, func(msg any) {
		if s, ok := msg.(string); ok {
			received <- s
		}
	})
	_ = localMed.Subscribe(ctx, "greetings", "", subPath)

	var topic typedpubsub.Topic[string]
	beh := topic.Behavior("greetings", localMed)

	beh(nil, typedpubsub.Publish[string]{Message: "hi"})
	beh(nil, typedpubsub.Publish[string]{Message: "bye"})

	var msgs []string
	for i := 0; i < 2; i++ {
		select {
		case m := <-received:
			msgs = append(msgs, m)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timed out waiting for message")
		}
	}
	if len(msgs) != 2 || msgs[0] != "hi" || msgs[1] != "bye" {
		t.Fatalf("unexpected messages: %v", msgs)
	}
}

// TestTopic_MultiplePublishes verifies multiple Publish commands are all forwarded.
func TestTopic_MultiplePublishes(t *testing.T) {
	med := &mockMediator{}
	var topic typedpubsub.Topic[int]
	beh := topic.Behavior("nums", med)

	for i := 0; i < 5; i++ {
		beh(nil, typedpubsub.Publish[int]{Message: i})
	}

	med.mu.Lock()
	defer med.mu.Unlock()
	if len(med.published) != 5 {
		t.Fatalf("expected 5 Publish calls, got %d", len(med.published))
	}
}

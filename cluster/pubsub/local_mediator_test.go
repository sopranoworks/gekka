/*
 * local_mediator_test.go
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

	"github.com/sopranoworks/gekka/cluster/pubsub"
)

func TestLocalMediator_PublishToSubscriber(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	received := make(chan any, 8)
	m.RegisterReceiver("/user/sub1", func(msg any) { received <- msg })

	if err := m.Subscribe(ctx, "events", "", "/user/sub1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := m.Publish(ctx, "events", "hello"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "hello" {
			t.Fatalf("expected 'hello', got %v", msg)
		}
	default:
		t.Fatal("no message received")
	}
}

func TestLocalMediator_PublishToMultipleSubscribers(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	var mu sync.Mutex
	var got []string

	for _, path := range []string{"/user/a", "/user/b", "/user/c"} {
		p := path
		m.RegisterReceiver(p, func(msg any) {
			mu.Lock()
			got = append(got, msg.(string))
			mu.Unlock()
		})
		_ = m.Subscribe(ctx, "news", "", p)
	}

	_ = m.Publish(ctx, "news", "broadcast")

	mu.Lock()
	n := len(got)
	mu.Unlock()

	if n != 3 {
		t.Fatalf("expected 3 deliveries, got %d", n)
	}
}

func TestLocalMediator_Unsubscribe(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	count := 0
	m.RegisterReceiver("/user/x", func(any) { count++ })

	_ = m.Subscribe(ctx, "topic", "", "/user/x")
	_ = m.Publish(ctx, "topic", "first")
	_ = m.Unsubscribe(ctx, "topic", "", "/user/x")
	_ = m.Publish(ctx, "topic", "second")

	if count != 1 {
		t.Fatalf("expected 1 delivery (before unsubscribe), got %d", count)
	}
}

func TestLocalMediator_DuplicateSubscribe(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	count := 0
	m.RegisterReceiver("/user/dup", func(any) { count++ })

	_ = m.Subscribe(ctx, "t", "", "/user/dup")
	_ = m.Subscribe(ctx, "t", "", "/user/dup") // duplicate
	_ = m.Publish(ctx, "t", "msg")

	if count != 1 {
		t.Fatalf("expected 1 delivery (no dup delivery), got %d", count)
	}
}

func TestLocalMediator_Send(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	received := make(chan any, 4)
	m.RegisterReceiver("/user/target", func(msg any) { received <- msg })

	_ = m.Send(ctx, "/user/target", "direct", false)

	select {
	case msg := <-received:
		if msg != "direct" {
			t.Fatalf("expected 'direct', got %v", msg)
		}
	default:
		t.Fatal("no message received via Send")
	}
}

func TestLocalMediator_PublishNoReceiver(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	// Subscribe without registering a receiver — should not panic.
	_ = m.Subscribe(ctx, "ghost", "", "/user/ghost")
	if err := m.Publish(ctx, "ghost", "msg"); err != nil {
		t.Fatalf("Publish to unregistered receiver must not error: %v", err)
	}
}

func TestLocalMediator_Subscribers(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	_ = m.Subscribe(ctx, "t", "", "/user/a")
	_ = m.Subscribe(ctx, "t", "", "/user/b")

	subs := m.Subscribers("t")
	if len(subs) != 2 {
		t.Fatalf("expected 2 subscribers, got %d", len(subs))
	}
}

func TestLocalMediator_TopicIsolation(t *testing.T) {
	m := pubsub.NewLocalMediator()
	ctx := context.Background()

	countA, countB := 0, 0
	m.RegisterReceiver("/user/a", func(any) { countA++ })
	m.RegisterReceiver("/user/b", func(any) { countB++ })

	_ = m.Subscribe(ctx, "topicA", "", "/user/a")
	_ = m.Subscribe(ctx, "topicB", "", "/user/b")

	_ = m.Publish(ctx, "topicA", "for-a")
	_ = m.Publish(ctx, "topicA", "for-a-2")

	if countA != 2 {
		t.Fatalf("topicA subscriber expected 2 messages, got %d", countA)
	}
	if countB != 0 {
		t.Fatalf("topicB subscriber expected 0 messages, got %d", countB)
	}
}

/*
 * cluster_mediator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster/pubsub"
)

func TestClusterMediator_GossipInterval(t *testing.T) {
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval: 50 * time.Millisecond,
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil
	})
	defer cm.Stop()

	if cm.GossipInterval() != 50*time.Millisecond {
		t.Fatalf("GossipInterval = %v, want 50ms", cm.GossipInterval())
	}
}

func TestClusterMediator_DefaultGossipInterval(t *testing.T) {
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{},
		func(ctx context.Context, path string, payload any) error { return nil },
		func() []string { return nil },
	)
	defer cm.Stop()

	if cm.GossipInterval() != 1*time.Second {
		t.Fatalf("GossipInterval = %v, want 1s (default)", cm.GossipInterval())
	}
}

func TestClusterMediator_SubscribeBumpsVersion(t *testing.T) {
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval: 10 * time.Second, // large so gossip doesn't fire during test
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil
	})
	defer cm.Stop()
	ctx := context.Background()

	if cm.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", cm.Version())
	}

	_ = cm.Subscribe(ctx, "topic", "", "/user/sub")
	if cm.Version() != 1 {
		t.Fatalf("version after subscribe = %d, want 1", cm.Version())
	}

	_ = cm.Unsubscribe(ctx, "topic", "", "/user/sub")
	if cm.Version() != 2 {
		t.Fatalf("version after unsubscribe = %d, want 2", cm.Version())
	}
}

func TestClusterMediator_PublishFansOut(t *testing.T) {
	var mu sync.Mutex
	var sentTo []string

	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval: 10 * time.Second,
	}, func(ctx context.Context, path string, payload any) error {
		mu.Lock()
		sentTo = append(sentTo, path)
		mu.Unlock()
		return nil
	}, func() []string {
		return []string{"/system/pubsub-mediator@node2", "/system/pubsub-mediator@node3"}
	})
	defer cm.Stop()
	ctx := context.Background()

	// Register local receiver.
	received := make(chan any, 4)
	cm.RegisterReceiver("/user/local", func(msg any) { received <- msg })
	_ = cm.Subscribe(ctx, "news", "", "/user/local")

	_ = cm.Publish(ctx, "news", "hello-cluster")

	// Local delivery should work.
	select {
	case msg := <-received:
		if msg != "hello-cluster" {
			t.Fatalf("local delivery: got %v, want 'hello-cluster'", msg)
		}
	default:
		t.Fatal("no local delivery")
	}

	// Should have sent to both peers.
	mu.Lock()
	n := len(sentTo)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 peer sends, got %d", n)
	}
}

func TestClusterMediator_GossipSendsStatus(t *testing.T) {
	var mu sync.Mutex
	var statusCount int

	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval: 30 * time.Millisecond,
	}, func(ctx context.Context, path string, payload any) error {
		if _, ok := payload.(pubsub.Status); ok {
			mu.Lock()
			statusCount++
			mu.Unlock()
		}
		return nil
	}, func() []string {
		return []string{"/system/pubsub-mediator@peer1"}
	})
	defer cm.Stop()

	// Wait for a few gossip rounds.
	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	count := statusCount
	mu.Unlock()

	if count < 2 {
		t.Fatalf("expected at least 2 gossip Status messages, got %d", count)
	}
}

func TestClusterMediator_Stop(t *testing.T) {
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval: 10 * time.Millisecond,
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil
	})

	// Stop should return quickly without blocking.
	done := make(chan struct{})
	go func() {
		cm.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s")
	}
}

// ── SendOne tests ──────────────────────────────────────────────────────

func TestLocalMediator_SendOne_Random(t *testing.T) {
	lm := pubsub.NewLocalMediator()
	var mu sync.Mutex
	var received []string
	lm.RegisterReceiver("sub1", func(msg any) {
		mu.Lock()
		received = append(received, "sub1")
		mu.Unlock()
	})
	lm.RegisterReceiver("sub2", func(msg any) {
		mu.Lock()
		received = append(received, "sub2")
		mu.Unlock()
	})
	ctx := context.Background()
	_ = lm.Subscribe(ctx, "topic", "", "sub1")
	_ = lm.Subscribe(ctx, "topic", "", "sub2")

	for i := 0; i < 100; i++ {
		lm.SendOne(ctx, "topic", "msg", "random")
	}

	mu.Lock()
	has1, has2 := false, false
	for _, r := range received {
		if r == "sub1" {
			has1 = true
		}
		if r == "sub2" {
			has2 = true
		}
	}
	mu.Unlock()
	if !has1 || !has2 {
		t.Errorf("expected both subscribers chosen, got sub1=%v sub2=%v", has1, has2)
	}
}

func TestLocalMediator_SendOne_RoundRobin(t *testing.T) {
	lm := pubsub.NewLocalMediator()
	var received []string
	lm.RegisterReceiver("sub1", func(msg any) { received = append(received, "sub1") })
	lm.RegisterReceiver("sub2", func(msg any) { received = append(received, "sub2") })
	ctx := context.Background()
	_ = lm.Subscribe(ctx, "topic", "", "sub1")
	_ = lm.Subscribe(ctx, "topic", "", "sub2")

	lm.SendOne(ctx, "topic", "msg", "round-robin")
	lm.SendOne(ctx, "topic", "msg", "round-robin")
	lm.SendOne(ctx, "topic", "msg", "round-robin")

	if len(received) != 3 {
		t.Fatalf("received %d, want 3", len(received))
	}
	// Round-robin should alternate.
	if received[0] == received[1] {
		t.Errorf("round-robin should alternate, got %v", received)
	}
}

func TestLocalMediator_SendOne_PerGroupRouting(t *testing.T) {
	lm := pubsub.NewLocalMediator()
	var received []string
	lm.RegisterReceiver("a1", func(msg any) { received = append(received, "a1") })
	lm.RegisterReceiver("a2", func(msg any) { received = append(received, "a2") })
	lm.RegisterReceiver("b1", func(msg any) { received = append(received, "b1") })
	ctx := context.Background()
	_ = lm.Subscribe(ctx, "topic", "groupA", "a1")
	_ = lm.Subscribe(ctx, "topic", "groupA", "a2")
	_ = lm.Subscribe(ctx, "topic", "groupB", "b1")

	lm.SendOne(ctx, "topic", "msg", "random")

	groupACount, groupBCount := 0, 0
	for _, r := range received {
		switch r {
		case "a1", "a2":
			groupACount++
		case "b1":
			groupBCount++
		}
	}
	if groupACount != 1 {
		t.Errorf("groupA deliveries = %d, want 1", groupACount)
	}
	if groupBCount != 1 {
		t.Errorf("groupB deliveries = %d, want 1", groupBCount)
	}
}

func TestLocalMediator_SendOne_NoSubscribers(t *testing.T) {
	lm := pubsub.NewLocalMediator()
	ctx := context.Background()
	delivered := lm.SendOne(ctx, "empty-topic", "msg", "random")
	if delivered {
		t.Error("SendOne should return false when no subscribers exist")
	}
}

// ── Dead letter tests ──────────────────────────────────────────────────

type mockEventStream struct {
	mu     sync.Mutex
	events []any
}

func (m *mockEventStream) Publish(event any) {
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
}

func (m *mockEventStream) Events() []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]any, len(m.events))
	copy(cp, m.events)
	return cp
}

func TestClusterMediator_Publish_DeadLetter(t *testing.T) {
	es := &mockEventStream{}
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval:                     10 * time.Second,
		SendToDeadLettersWhenNoSubscribers: true,
		EventStream:                        es,
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil // no peers
	})
	defer cm.Stop()

	_ = cm.Publish(context.Background(), "empty-topic", "orphan-msg")

	events := es.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(events))
	}
	dl, ok := events[0].(pubsub.DeadLetterPubSub)
	if !ok {
		t.Fatalf("expected DeadLetterPubSub, got %T", events[0])
	}
	if dl.Topic != "empty-topic" || dl.Message != "orphan-msg" {
		t.Errorf("dead letter = %+v", dl)
	}
}

func TestClusterMediator_Publish_NoDeadLetterWhenDisabled(t *testing.T) {
	es := &mockEventStream{}
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval:                     10 * time.Second,
		SendToDeadLettersWhenNoSubscribers: false,
		EventStream:                        es,
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil
	})
	defer cm.Stop()

	_ = cm.Publish(context.Background(), "topic", "msg")

	if len(es.Events()) != 0 {
		t.Errorf("expected no dead letters when disabled, got %d", len(es.Events()))
	}
}

func TestClusterMediator_SendOne_DeadLetter(t *testing.T) {
	es := &mockEventStream{}
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval:                     10 * time.Second,
		SendToDeadLettersWhenNoSubscribers: true,
		EventStream:                        es,
	}, func(ctx context.Context, path string, payload any) error {
		return nil
	}, func() []string {
		return nil
	})
	defer cm.Stop()

	_ = cm.SendOne(context.Background(), "no-subs-topic", "orphan")

	events := es.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(events))
	}
	dl := events[0].(pubsub.DeadLetterPubSub)
	if dl.Topic != "no-subs-topic" {
		t.Errorf("dead letter topic = %q, want %q", dl.Topic, "no-subs-topic")
	}
}

// ── Tombstone test ─────────────────────────────────────────────────────

func TestClusterMediator_TombstonePreventsGossip(t *testing.T) {
	var mu sync.Mutex
	var deltas []pubsub.Delta

	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
		GossipInterval:    10 * time.Second,
		RemovedTimeToLive: 1 * time.Hour, // won't expire during test
	}, func(ctx context.Context, path string, payload any) error {
		mu.Lock()
		if d, ok := payload.(pubsub.Delta); ok {
			deltas = append(deltas, d)
		}
		mu.Unlock()
		return nil
	}, func() []string {
		return []string{"peer1"}
	})
	defer cm.Stop()

	ctx := context.Background()
	cm.RegisterReceiver("/user/sub", func(msg any) {})
	_ = cm.Subscribe(ctx, "topic", "", "/user/sub")
	_ = cm.Unsubscribe(ctx, "topic", "", "/user/sub")

	// Re-subscribe with a different path so topic still has entries.
	cm.RegisterReceiver("/user/sub2", func(msg any) {})
	_ = cm.Subscribe(ctx, "topic", "", "/user/sub2")

	// Trigger a HandleStatus to force a delta send.
	cm.HandleStatus(ctx, "peer1", pubsub.Status{
		Versions: map[pubsub.Address]int64{
			{}: 0, // peer is behind
		},
	})

	mu.Lock()
	defer mu.Unlock()
	if len(deltas) == 0 {
		t.Fatal("expected at least one delta")
	}
	// The tombstoned "/user/sub" should not appear in the delta content.
	for _, d := range deltas {
		for _, b := range d.Buckets {
			for _, vh := range b.Content {
				if vh.Ref == "/user/sub" {
					t.Error("tombstoned path /user/sub should not appear in delta")
				}
			}
		}
	}
}

// ── Config defaults test ───────────────────────────────────────────────

func TestClusterMediator_ConfigDefaults(t *testing.T) {
	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{},
		func(ctx context.Context, path string, payload any) error { return nil },
		func() []string { return nil },
	)
	defer cm.Stop()

	if cm.GossipInterval() != 1*time.Second {
		t.Errorf("GossipInterval = %v, want 1s", cm.GossipInterval())
	}
	// Config() is not exported, but we can verify defaults via the name accessor.
	if cm.Name() != "distributedPubSubMediator" {
		t.Errorf("Name = %q, want %q", cm.Name(), "distributedPubSubMediator")
	}
}

// ── DeadLetterPubSub type test ─────────────────────────────────────────

func TestDeadLetterPubSub_Fields(t *testing.T) {
	dl := pubsub.DeadLetterPubSub{
		Message: "test",
		Topic:   "my-topic",
		Cause:   "no-subscribers",
	}
	typ := reflect.TypeOf(dl)
	if typ.NumField() != 3 {
		t.Errorf("DeadLetterPubSub has %d fields, want 3", typ.NumField())
	}
}

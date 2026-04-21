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

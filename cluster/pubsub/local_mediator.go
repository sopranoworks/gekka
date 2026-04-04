/*
 * local_mediator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub

import (
	"context"
	"sync"
)

// subEntry holds one subscription record.
type subEntry struct {
	path  string
	group string
}

// LocalMediator is a concrete in-process implementation of the Mediator interface.
// Messages are delivered synchronously within the same process via registered receiver
// functions. Suitable for single-node deployments and unit tests.
//
// Usage:
//
//	m := pubsub.NewLocalMediator()
//	ch := make(chan any, 16)
//	m.RegisterReceiver("/user/sub", func(msg any) { ch <- msg })
//	m.Subscribe(ctx, "events", "", "/user/sub")
//	m.Publish(ctx, "events", "hello")  // ch receives "hello"
type LocalMediator struct {
	mu        sync.RWMutex
	subs      map[string][]subEntry   // topic → subscriptions
	receivers map[string]func(msg any) // receiverPath → delivery function
}

// NewLocalMediator returns an initialised LocalMediator.
func NewLocalMediator() *LocalMediator {
	return &LocalMediator{
		subs:      make(map[string][]subEntry),
		receivers: make(map[string]func(any)),
	}
}

// RegisterReceiver registers a delivery function for the given actor path.
// The function is invoked by Publish / Send when a message is routed to that path.
// Must be called before Subscribe to enable delivery.
func (m *LocalMediator) RegisterReceiver(path string, fn func(msg any)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receivers[path] = fn
}

// UnregisterReceiver removes the delivery function for the given actor path.
func (m *LocalMediator) UnregisterReceiver(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.receivers, path)
}

// Publish delivers msg to every subscriber registered for topic.
// Delivery is synchronous; all registered receiver functions are called before
// Publish returns. When a subscriber path has no registered receiver the message
// is silently dropped (as would happen with a dead-letter remote actor).
func (m *LocalMediator) Publish(_ context.Context, topic string, msg any) error {
	m.mu.RLock()
	entries := make([]subEntry, len(m.subs[topic]))
	copy(entries, m.subs[topic])
	receivers := m.receivers
	m.mu.RUnlock()

	for _, e := range entries {
		if fn, ok := receivers[e.path]; ok {
			fn(msg)
		}
	}
	return nil
}

// Send delivers msg to exactly one subscriber registered at path.
// When localAffinity is true, it is honoured trivially (there is only one node).
func (m *LocalMediator) Send(_ context.Context, path string, msg any, _ bool) error {
	m.mu.RLock()
	fn, ok := m.receivers[path]
	m.mu.RUnlock()
	if ok {
		fn(msg)
	}
	return nil
}

// Subscribe registers receiverPath as a subscriber for topic (and optional group).
// Duplicate subscriptions (same topic + path) are ignored.
func (m *LocalMediator) Subscribe(_ context.Context, topic, group, receiverPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.subs[topic] {
		if e.path == receiverPath && e.group == group {
			return nil
		}
	}
	m.subs[topic] = append(m.subs[topic], subEntry{path: receiverPath, group: group})
	return nil
}

// Unsubscribe removes receiverPath from topic (and optional group).
func (m *LocalMediator) Unsubscribe(_ context.Context, topic, group, receiverPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	subs := m.subs[topic]
	for i, e := range subs {
		if e.path == receiverPath && e.group == group {
			m.subs[topic] = append(subs[:i], subs[i+1:]...)
			return nil
		}
	}
	return nil
}

// Subscribers returns the current subscriber paths for a topic (for testing/inspection).
func (m *LocalMediator) Subscribers(topic string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := m.subs[topic]
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = e.path
	}
	return paths
}

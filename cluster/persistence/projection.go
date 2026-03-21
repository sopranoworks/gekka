/*
 * projection.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"fmt"
	"sync"
)

// ── OffsetStore ───────────────────────────────────────────────────────────────

// OffsetStore persists and retrieves the last-consumed Offset for a named
// projection.  The stored value is the next Offset to read (i.e. one past the
// last processed event's Offset).
type OffsetStore interface {
	// Save stores nextOffset as the resume point for projectionId.
	Save(projectionId string, nextOffset Offset) error
	// Load returns the last saved resume offset, or 0 if none exists.
	Load(projectionId string) (Offset, error)
}

// InMemoryOffsetStore is a thread-safe, in-process OffsetStore suitable for
// testing and single-node deployments where durability is not required.
type InMemoryOffsetStore struct {
	mu      sync.Mutex
	offsets map[string]Offset
}

// NewInMemoryOffsetStore returns an empty InMemoryOffsetStore.
func NewInMemoryOffsetStore() *InMemoryOffsetStore {
	return &InMemoryOffsetStore{offsets: make(map[string]Offset)}
}

func (s *InMemoryOffsetStore) Save(projectionId string, nextOffset Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[projectionId] = nextOffset
	return nil
}

func (s *InMemoryOffsetStore) Load(projectionId string) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets[projectionId], nil // zero value (0) when absent
}

// ── Projection ────────────────────────────────────────────────────────────────

// Projection polls a ReadJournal for events carrying a specific tag, calls a
// user-defined Handler for each event in global append order, and saves its
// position to an OffsetStore after each successful event.
//
// Delivery guarantee: at-least-once.  If the process crashes between calling
// Handler and saving the Offset, the event will be re-delivered on the next
// RunOnce call.
//
// Usage:
//
//	p := NewProjection("my-read-model", "order", rj, handler, offsetStore)
//	if err := p.RunOnce(); err != nil { ... }
type Projection struct {
	id          string
	tag         string
	journal     ReadJournal
	handler     func(TaggedEvent) error
	offsetStore OffsetStore
}

// NewProjection creates a Projection that processes events tagged with tag.
//
//   - id is the unique name of this projection (used as the OffsetStore key).
//   - tag is the event tag to subscribe to.
//   - journal is the ReadJournal to poll.
//   - handler is called for each new event; a non-nil error stops processing.
//   - offsetStore persists the projection's read position across restarts.
func NewProjection(
	id string,
	tag string,
	journal ReadJournal,
	handler func(TaggedEvent) error,
	offsetStore OffsetStore,
) *Projection {
	return &Projection{
		id:          id,
		tag:         tag,
		journal:     journal,
		handler:     handler,
		offsetStore: offsetStore,
	}
}

// RunOnce processes all events that are available since the last saved Offset.
//
// For each event the Handler is called and, on success, the Offset is
// advanced by saving Offset+1 to the OffsetStore.  RunOnce is idempotent:
// calling it when there are no new events is a no-op.
func (p *Projection) RunOnce() error {
	fromOffset, err := p.offsetStore.Load(p.id)
	if err != nil {
		return fmt.Errorf("projection %q: load offset: %w", p.id, err)
	}

	events, err := p.journal.EventsByTag(p.tag, fromOffset)
	if err != nil {
		return fmt.Errorf("projection %q: EventsByTag: %w", p.id, err)
	}

	for _, ev := range events {
		if err := p.handler(ev); err != nil {
			return fmt.Errorf("projection %q: handler at offset %d: %w", p.id, ev.Offset, err)
		}
		if err := p.offsetStore.Save(p.id, ev.Offset+1); err != nil {
			return fmt.Errorf("projection %q: save offset: %w", p.id, err)
		}
	}
	return nil
}

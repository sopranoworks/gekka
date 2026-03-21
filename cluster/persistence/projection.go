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
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/cluster/singleton"
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

// RunOnce processes all events that have been written since the last saved Offset.
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

// ── ProjectionActor ───────────────────────────────────────────────────────────

// projectionTickMsg is the self-message that drives periodic polling.
type projectionTickMsg struct{}

// StopProjection is sent to a ProjectionActor to request a graceful shutdown.
// The actor performs one final RunOnce() to flush any events that arrived
// since the last tick before stopping itself.
type StopProjection struct{}

// ProjectionActor wraps a Projection as a classic actor.  It self-schedules a
// tick every interval and calls RunOnce on each tick.  On StopProjection it
// performs a final RunOnce (flush) then stops itself.
//
// Exactly one instance of a ProjectionActor should run in the cluster at any
// time.  Use StartDistributedProjection to enforce this via a ClusterSingleton.
type ProjectionActor struct {
	actor.BaseActor
	proj     *Projection
	interval time.Duration
}

// NewProjectionActor creates a ProjectionActor.
// When interval <= 0 the default of 500 ms is used.
func NewProjectionActor(proj *Projection, interval time.Duration) *ProjectionActor {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &ProjectionActor{
		BaseActor: actor.NewBaseActor(),
		proj:      proj,
		interval:  interval,
	}
}

// PreStart arms the first polling tick.
func (a *ProjectionActor) PreStart() {
	a.scheduleTick()
}

// scheduleTick fires a projectionTickMsg to self after one interval.
func (a *ProjectionActor) scheduleTick() {
	self := a.Self()
	time.AfterFunc(a.interval, func() {
		self.Tell(projectionTickMsg{})
	})
}

// Receive handles tick and stop messages.
func (a *ProjectionActor) Receive(msg any) {
	switch msg.(type) {
	case projectionTickMsg:
		if err := a.proj.RunOnce(); err != nil {
			a.Log().Error("ProjectionActor: poll failed", "error", err)
		}
		a.scheduleTick()

	case StopProjection:
		// Final flush: capture any events that arrived since the last tick.
		if err := a.proj.RunOnce(); err != nil {
			a.Log().Error("ProjectionActor: final flush on stop failed", "error", err)
		}
		if s, ok := a.System().(interface{ Stop(actor.Ref) }); ok {
			s.Stop(a.Self())
		}
	}
}

// ── StartDistributedProjection ────────────────────────────────────────────────

// StartDistributedProjection registers a ProjectionActor as a ClusterSingleton
// so that exactly one instance runs in the cluster at any time.
//
// When the oldest node leaves or crashes, ClusterSingletonManager automatically
// spawns the actor on the new oldest node.  Because both nodes share the same
// OffsetStore the new instance resumes from exactly where the previous one left
// off, providing HA with at-least-once delivery.
//
// Parameters:
//   - sys         the actor system used to register the singleton manager
//   - cm          the ClusterManager used by the singleton to track leadership
//   - name        unique name for this projection (also the OffsetStore key)
//   - tag         the event tag this projection subscribes to
//   - journal     the ReadJournal to poll
//   - handler     called for each tagged event
//   - offsetStore durable store for the read position; must be shared across all nodes
func StartDistributedProjection(
	sys actor.ActorContext,
	cm *cluster.ClusterManager,
	name string,
	tag string,
	journal ReadJournal,
	handler func(TaggedEvent) error,
	offsetStore OffsetStore,
) (actor.Ref, error) {
	proj := NewProjection(name, tag, journal, handler, offsetStore)
	singletonProps := actor.Props{
		New: func() actor.Actor {
			return NewProjectionActor(proj, 500*time.Millisecond)
		},
	}
	mgrProps := actor.Props{
		New: func() actor.Actor {
			return singleton.NewClusterSingletonManager(cm, singletonProps, "")
		},
	}
	return sys.ActorOf(mgrProps, "projectionManager-"+name)
}

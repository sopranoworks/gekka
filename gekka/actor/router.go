/*
 * router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

// RoutingLogic defines the strategy used to select a target routee for each
// message. Implement this interface to add custom distribution algorithms
// (consistent-hash, smallest-mailbox, etc.).
//
// Select must be safe for concurrent use from multiple goroutines.
type RoutingLogic interface {
	// Select chooses one Ref from routees based on message and the
	// implementation's internal state. Returns nil only when routees is empty.
	Select(message any, routees []Ref) Ref
}

// Broadcast is a wrapper message that instructs the RouterActor to deliver
// the inner Message to every routee, bypassing the RoutingLogic selector.
//
//	router.Tell(actor.Broadcast{Message: []byte("ping")})
type Broadcast struct {
	// Message is the payload forwarded to every routee.
	Message any
}

// RouterActor forwards incoming messages to a set of routees according to a
// pluggable RoutingLogic. It implements the Actor interface by embedding
// BaseActor, so it can be registered and started like any other actor.
//
// Create one with NewRouterActor and register it via node.SpawnActor or
// node.System.ActorOf. Routees are plain actor.Ref values, so they can be
// local or remote actors (including gekka.ActorRef).
//
// Design notes for future extension:
//
//   - Pool routers will override Receive to intercept management messages
//     (e.g. AdjustPoolSize) and spawn/stop child routees via System().ActorOf.
//   - Group routers will resolve routee refs by path at message-delivery time
//     via System().ActorOf / ActorSelection rather than storing a fixed slice.
type RouterActor struct {
	BaseActor
	Logic   RoutingLogic
	Routees []Ref
}

// NewRouterActor creates a RouterActor with the given routing logic and an
// initial set of routees. The routees slice is copied so later mutations to
// the caller's slice do not affect the router.
func NewRouterActor(logic RoutingLogic, routees []Ref) *RouterActor {
	cp := make([]Ref, len(routees))
	copy(cp, routees)
	return &RouterActor{
		BaseActor: NewBaseActor(),
		Logic:     logic,
		Routees:   cp,
	}
}

// Routees returns a snapshot of the current routee list.
func (r *RouterActor) RouteesSnapshot() []Ref {
	cp := make([]Ref, len(r.Routees))
	copy(cp, r.Routees)
	return cp
}

// AddRoutee appends ref to the routee list. Safe to call from Receive; not
// safe to call concurrently from outside the actor's goroutine.
func (r *RouterActor) AddRoutee(ref Ref) {
	r.Routees = append(r.Routees, ref)
}

// RemoveRoutee removes the first routee whose path matches ref.Path().
// No-op when ref is not in the list.
func (r *RouterActor) RemoveRoutee(ref Ref) {
	for i, rt := range r.Routees {
		if rt.Path() == ref.Path() {
			r.Routees = append(r.Routees[:i], r.Routees[i+1:]...)
			return
		}
	}
}

// Receive handles incoming messages.
//
// A Broadcast wrapper delivers Message to every routee (ignoring the logic).
// Any other value is forwarded to the single routee selected by logic.Select.
// The original sender (r.Sender()) is preserved in both cases.
func (r *RouterActor) Receive(msg any) {
	switch m := msg.(type) {
	case Broadcast:
		// Deliver inner message to all routees, preserving sender.
		for _, rt := range r.Routees {
			rt.Tell(m.Message, r.Sender())
		}
	default:
		if len(r.Routees) == 0 {
			return
		}
		target := r.Logic.Select(msg, r.Routees)
		if target != nil {
			target.Tell(msg, r.Sender())
		}
	}
}

// ── Pool router ───────────────────────────────────────────────────────────────

// AdjustPoolSize is a management message sent to a PoolRouter to request a
// change in the number of active routees.
//
// Positive Delta adds new children; negative Delta is reserved for future
// graceful-shrink support.
//
//	router.Tell(actor.AdjustPoolSize{Delta: 2}) // grow pool by 2
type AdjustPoolSize struct {
	// Delta is the signed change in pool size. Only positive values are acted
	// upon in the current implementation.
	Delta int
}

// PoolRouter is a RouterActor that owns and manages its routees as child actors.
//
// Children are spawned during PreStart using the configured Props. When a child
// stops, the router receives a TerminatedMessage and removes that routee from
// the active list. The pool can be grown at runtime by sending AdjustPoolSize.
//
// The PoolRouter uses the parent path of its own registered path as the
// namespace for children, e.g. for a pool at /user/pool the children will be
// at /user/pool/$pool-0, /user/pool/$pool-1, etc.
//
// Usage:
//
//	pool := actor.NewPoolRouter(
//	    &actor.RoundRobinRoutingLogic{},
//	    3,
//	    actor.Props{New: func() actor.Actor {
//	        return &WorkerActor{BaseActor: actor.NewBaseActor()}
//	    }},
//	)
//	node.System.ActorOf(actor.Props{New: func() actor.Actor { return pool }}, "pool")
type PoolRouter struct {
	RouterActor
	nrOfInstances int
	props         Props
}

// NrOfInstances returns the current configured pool size (the count that was
// set at creation or updated by AdjustPoolSize). It reflects the target size,
// not the number of currently live routees.
func (r *PoolRouter) NrOfInstances() int { return r.nrOfInstances }

// NewPoolRouter creates a PoolRouter that will spawn nrOfInstances children
// in PreStart, routing normal messages using logic.
func NewPoolRouter(logic RoutingLogic, nrOfInstances int, props Props) *PoolRouter {
	return &PoolRouter{
		RouterActor:   RouterActor{BaseActor: NewBaseActor(), Logic: logic},
		nrOfInstances: nrOfInstances,
		props:         props,
	}
}

// PreStart is called by Start before the first message is delivered.
// It spawns nrOfInstances child actors and registers this pool as a watcher
// of each, so the pool receives a TerminatedMessage when any child stops.
func (r *PoolRouter) PreStart() {
	sys := r.System()
	if sys == nil || r.nrOfInstances <= 0 || r.props.New == nil {
		return
	}
	r.Routees = make([]Ref, 0, r.nrOfInstances)
	for i := 0; i < r.nrOfInstances; i++ {
		name := fmt.Sprintf("$pool-%d", i)
		ref, err := sys.ActorOf(r.props, name)
		if err != nil {
			continue
		}
		r.Routees = append(r.Routees, ref)
		sys.Watch(r.Self(), ref)
	}
}

// Receive handles lifecycle and management messages before delegating to the
// embedded RouterActor.
//
//   - TerminatedMessage: removes the stopped routee from the active list.
//   - AdjustPoolSize: spawns additional children (positive Delta only).
//   - Everything else: forwarded via RouterActor.Receive.
func (r *PoolRouter) Receive(msg any) {
	switch m := msg.(type) {
	case TerminatedMessage:
		// A watched child has stopped — evict it from the routee list.
		r.RemoveRoutee(m.TerminatedActor())

	case AdjustPoolSize:
		// Grow-only resize for now; shrink support is future work.
		if m.Delta <= 0 || r.System() == nil || r.props.New == nil {
			return
		}
		for i := 0; i < m.Delta; i++ {
			name := fmt.Sprintf("$pool-%d", r.nrOfInstances+i)
			ref, err := r.System().ActorOf(r.props, name)
			if err != nil {
				continue
			}
			r.Routees = append(r.Routees, ref)
			r.System().Watch(r.Self(), ref)
		}
		r.nrOfInstances += m.Delta

	default:
		r.RouterActor.Receive(msg)
	}
}

// ── Routing strategies ────────────────────────────────────────────────────────

// RoundRobinRoutingLogic distributes messages to routees in a rotating,
// sequential order. The counter is managed with sync/atomic so multiple
// goroutines can call Select concurrently without a mutex.
type RoundRobinRoutingLogic struct {
	counter atomic.Uint64
}

// Select returns the next routee in the round-robin sequence.
func (l *RoundRobinRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	idx := l.counter.Add(1) - 1
	return routees[idx%uint64(len(routees))]
}

// RandomRoutingLogic selects a uniformly random routee for each message.
// It is safe for concurrent use from multiple goroutines (Go 1.20+
// global rand source is automatically seeded and goroutine-safe).
type RandomRoutingLogic struct{}

// Select returns a randomly chosen routee, or nil when routees is empty.
func (l *RandomRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	return routees[rand.Intn(len(routees))]
}

// ── Consistent Hashing ───────────────────────────────────────────────────────

// ConsistentHashable is implemented by messages that want to provide an
// explicit key for consistent-hash routing. If a message does not implement
// this interface, the message itself is used as the key.
type ConsistentHashable interface {
	// ConsistentHashKey returns the value used to hash this message.
	ConsistentHashKey() any
}

type nodeHash struct {
	hash uint32
	ref  Ref
}

// ConsistentHashRoutingLogic maps messages to routees based on a hash key.
// Messages with the same key are consistently delivered to the same routee,
// provided the set of routees remains stable.
//
// It follows the standard consistent hashing algorithm with virtual nodes
// for improved distribution balance.
type ConsistentHashRoutingLogic struct {
	// VirtualNodesFactor determines how many tokens each routee gets on the
	// hash ring (default 10). A higher factor improves balance but increases
	// memory and CPU for ring rebuilding.
	VirtualNodesFactor int

	mu              sync.Mutex
	ring            []nodeHash
	lastRouteesHash uint64
}

// Select returns a routee mapped from the message's hash key.
func (l *ConsistentHashRoutingLogic) Select(message any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Rebuild ring if routees changed
	h := l.calculateRouteesHash(routees)
	if h != l.lastRouteesHash || len(l.ring) == 0 {
		l.rebuildRing(routees)
		l.lastRouteesHash = h
	}

	// 2. Extract key
	var key any = message
	if ch, ok := message.(ConsistentHashable); ok {
		key = ch.ConsistentHashKey()
	}

	// 3. Hash the key
	keyHash := l.hashKey(key)

	// 4. Binary search on the ring
	idx := sort.Search(len(l.ring), func(i int) bool {
		return l.ring[i].hash >= keyHash
	})

	if idx == len(l.ring) {
		idx = 0
	}
	return l.ring[idx].ref
}

func (l *ConsistentHashRoutingLogic) calculateRouteesHash(routees []Ref) uint64 {
	// Simple hash of paths and length for change detection.
	var h uint64 = uint64(len(routees))
	for _, r := range routees {
		p := r.Path()
		for i := 0; i < len(p); i++ {
			h = h*31 + uint64(p[i])
		}
	}
	return h
}

func (l *ConsistentHashRoutingLogic) rebuildRing(routees []Ref) {
	vnodes := l.VirtualNodesFactor
	if vnodes <= 0 {
		vnodes = 10
	}

	l.ring = make([]nodeHash, 0, len(routees)*vnodes)
	for _, r := range routees {
		path := r.Path()
		for i := 0; i < vnodes; i++ {
			// Virtual node key is "path#index"
			vnodeKey := fmt.Sprintf("%s#%d", path, i)
			h := crc32.ChecksumIEEE([]byte(vnodeKey))
			l.ring = append(l.ring, nodeHash{hash: h, ref: r})
		}
	}

	// Sort by hash to enable binary search.
	sort.Slice(l.ring, func(i, j int) bool {
		return l.ring[i].hash < l.ring[j].hash
	})
}

func (l *ConsistentHashRoutingLogic) hashKey(key any) uint32 {
	switch v := key.(type) {
	case string:
		return crc32.ChecksumIEEE([]byte(v))
	case []byte:
		return crc32.ChecksumIEEE(v)
	case int, int32, int64, uint, uint32, uint64:
		return crc32.ChecksumIEEE([]byte(fmt.Sprint(v)))
	default:
		return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%v", v)))
	}
}

// ── Group router ──────────────────────────────────────────────────────────────

// GroupRouter routes messages to a fixed set of pre-existing actor references.
// Unlike PoolRouter it does not spawn or supervise routees — it treats them as
// external actors whose lifecycle it does not control.
//
// Routees can be supplied either as live Refs (NewGroupRouter) or as path
// strings resolved at startup time (NewGroupRouterWithPaths). Path resolution
// is performed during PreStart using ActorContext.Resolve; any path that
// cannot be resolved at that moment is silently skipped.
//
// Usage — explicit refs:
//
//	group := actor.NewGroupRouter(
//	    &actor.RoundRobinRoutingLogic{},
//	    []actor.Ref{ref1, ref2, ref3},
//	)
//	node.System.ActorOf(actor.Props{New: func() actor.Actor { return group }}, "group")
//
// Usage — HOCON deployment (paths resolved automatically by the system):
//
//	pekko.actor.deployment {
//	  "/user/group" {
//	    router        = round-robin-group
//	    routees.paths = ["/user/worker1", "/user/worker2"]
//	  }
//	}
type GroupRouter struct {
	RouterActor
	Paths []string // paths resolved in PreStart; empty when refs were supplied directly
}

// NewGroupRouter creates a GroupRouter pre-loaded with the supplied routee Refs.
// The slice is copied so later mutations by the caller do not affect the router.
func NewGroupRouter(logic RoutingLogic, routees []Ref) *GroupRouter {
	cp := make([]Ref, len(routees))
	copy(cp, routees)
	return &GroupRouter{
		RouterActor: RouterActor{BaseActor: NewBaseActor(), Logic: logic, Routees: cp},
	}
}

// NewGroupRouterWithPaths creates a GroupRouter that resolves its routees from
// path strings during PreStart. Paths are looked up via ActorContext.Resolve;
// unreachable paths are skipped without error.
func NewGroupRouterWithPaths(logic RoutingLogic, paths []string) *GroupRouter {
	cp := make([]string, len(paths))
	copy(cp, paths)
	return &GroupRouter{
		RouterActor: RouterActor{BaseActor: NewBaseActor(), Logic: logic},
		Paths:       cp,
	}
}

// RouteePathsForTest returns a snapshot of the configured routee paths.
// Intended for test assertions; prefer Routees() for the resolved live refs.
func (r *GroupRouter) RouteePathsForTest() []string {
	cp := make([]string, len(r.Paths))
	copy(cp, r.Paths)
	return cp
}

// PreStart resolves routeePaths into live Refs via ActorContext.Resolve.
// Paths that cannot be resolved (e.g. actor not yet started) are skipped.
// If routeePaths is empty (refs were supplied at construction time) this is
// a no-op.
func (r *GroupRouter) PreStart() {
	sys := r.System()
	if sys == nil || len(r.Paths) == 0 {
		return
	}
	for _, path := range r.Paths {
		ref, err := sys.Resolve(path)
		if err != nil {
			continue
		}
		r.Routees = append(r.Routees, ref)
	}
}

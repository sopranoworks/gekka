/*
 * replicator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ReplicatorMsg is the JSON envelope sent over Artery for gossip.
type ReplicatorMsg struct {
	Type    string          `json:"type"` // "gcounter-gossip", "orset-gossip", "lwwmap-gossip", "get-reply"
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

// GCounterPayload carries a GCounter snapshot.
type GCounterPayload struct {
	State map[string]uint64 `json:"state"`
}

// ORSetPayload carries an ORSet snapshot.
type ORSetPayload struct {
	ORSetSnapshot
}

// LWWMapPayload carries an LWWMap snapshot.
type LWWMapPayload struct {
	State map[string]LWWEntry `json:"state"`
}

// PNCounterPayload carries a PNCounter snapshot.
type PNCounterPayload struct {
	PNCounterSnapshot
}

// LWWRegisterPayload carries a LWWRegister snapshot.
type LWWRegisterPayload struct {
	LWWRegisterSnapshot
}

// Consistency levels for Write operations.
type WriteConsistency int

const (
	WriteLocal WriteConsistency = iota
	WriteAll
)

// Replicator manages a set of named CRDTs and gossips state to peers.
type Replicator struct {
	mu       sync.RWMutex
	nodeID   string
	counters   map[string]*GCounter
	sets       map[string]*ORSet
	maps       map[string]*LWWMap
	pnCounters map[string]*PNCounter
	orFlags    map[string]*ORFlag
	registers  map[string]*LWWRegister

	peers  []peerInfo // remote actor paths to gossip to
	router *actor.Router

	GossipInterval time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup

	// Callback invoked when a gossip message arrives for a key we don't know about.
	OnUnknownKey func(key string, msg ReplicatorMsg)
}

type peerInfo struct {
	path string // pekko://System@host:port/user/goReplicator
}

// NewReplicator creates a Replicator for the given nodeID.
func NewReplicator(nodeID string, router *actor.Router) *Replicator {
	return &Replicator{
		nodeID:         nodeID,
		counters:       make(map[string]*GCounter),
		sets:           make(map[string]*ORSet),
		maps:           make(map[string]*LWWMap),
		pnCounters:     make(map[string]*PNCounter),
		orFlags:        make(map[string]*ORFlag),
		registers:      make(map[string]*LWWRegister),
		router:         router,
		GossipInterval: 2 * time.Second,
		stopCh:         make(chan struct{}),
	}
}

// AddPeer registers a remote replicator actor path.
func (r *Replicator) AddPeer(actorPath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = append(r.peers, peerInfo{path: actorPath})
}

// GCounter returns (or creates) the named GCounter.
func (r *Replicator) GCounter(key string) *GCounter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[key]; ok {
		return c
	}
	c := NewGCounter()
	r.counters[key] = c
	return c
}

// ORSet returns (or creates) the named ORSet.
func (r *Replicator) ORSet(key string) *ORSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sets[key]; ok {
		return s
	}
	s := NewORSet()
	r.sets[key] = s
	return s
}

// LWWMap returns (or creates) the named LWWMap.
func (r *Replicator) LWWMap(key string) *LWWMap {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.maps[key]; ok {
		return m
	}
	m := NewLWWMap()
	r.maps[key] = m
	return m
}

// PNCounter returns (or creates) the named PNCounter.
func (r *Replicator) PNCounter(key string) *PNCounter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.pnCounters[key]; ok {
		return c
	}
	c := NewPNCounter()
	r.pnCounters[key] = c
	return c
}

// ORFlag returns (or creates) the named ORFlag.
func (r *Replicator) ORFlag(key string) *ORFlag {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.orFlags[key]; ok {
		return f
	}
	f := NewORFlag()
	r.orFlags[key] = f
	return f
}

// LWWRegister returns (or creates) the named LWWRegister.
func (r *Replicator) LWWRegister(key string) *LWWRegister {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reg, ok := r.registers[key]; ok {
		return reg
	}
	reg := NewLWWRegister()
	r.registers[key] = reg
	return reg
}

// IncrementCounter increments the named counter for this node.
func (r *Replicator) IncrementCounter(key string, delta uint64, consistency WriteConsistency) {
	c := r.GCounter(key)
	c.Increment(r.nodeID, delta)
	if consistency == WriteAll {
		r.gossipCounter(context.Background(), key, c)
	}
}

// AddToSet adds element to the named set.
func (r *Replicator) AddToSet(key, element string, consistency WriteConsistency) {
	s := r.ORSet(key)
	s.Add(r.nodeID, element)
	if consistency == WriteAll {
		r.gossipSet(context.Background(), key, s)
	}
}

// RemoveFromSet removes element from the named set.
func (r *Replicator) RemoveFromSet(key, element string, consistency WriteConsistency) {
	s := r.ORSet(key)
	s.Remove(element)
	if consistency == WriteAll {
		r.gossipSet(context.Background(), key, s)
	}
}

// PutInMap adds/updates a value in the named LWWMap.
func (r *Replicator) PutInMap(key, itemKey string, value any, consistency WriteConsistency) {
	m := r.LWWMap(key)
	m.Put(itemKey, value)
	if consistency == WriteAll {
		r.gossipMap(context.Background(), key, m)
	}
}

// Start begins the periodic gossip loop.
func (r *Replicator) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.GossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.gossipAll(ctx)
			}
		}
	}()
}

// AllSetsSnapshot returns a point-in-time copy of all ORSets currently known
// to this Replicator.  Keys are set names; values are element slices.
// Safe to call from any goroutine.
func (r *Replicator) AllSetsSnapshot() map[string][]string {
	r.mu.RLock()
	sets := make(map[string]*ORSet, len(r.sets))
	for k, v := range r.sets {
		sets[k] = v
	}
	r.mu.RUnlock()

	result := make(map[string][]string, len(sets))
	for key, s := range sets {
		result[key] = s.Elements()
	}
	return result
}

// Stop halts the gossip loop.
func (r *Replicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// HandleIncoming processes a raw JSON message from a peer.
// Call this from your NodeManager's UserMessageCallback.
func (r *Replicator) HandleIncoming(data []byte) error {
	var msg ReplicatorMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("replicator: unmarshal: %w", err)
	}
	switch msg.Type {
	case "gcounter-gossip":
		var p GCounterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		c := r.GCounter(msg.Key)
		c.MergeState(p.State)
		log.Printf("Replicator: merged GCounter[%s], value=%d", msg.Key, c.Value())

	case "orset-gossip":
		var snap ORSetSnapshot
		if err := json.Unmarshal(msg.Payload, &snap); err != nil {
			return err
		}
		s := r.ORSet(msg.Key)
		s.MergeSnapshot(snap)
		log.Printf("Replicator: merged ORSet[%s], elements=%v", msg.Key, s.Elements())

	case "lwwmap-gossip":
		var p LWWMapPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		m := r.LWWMap(msg.Key)
		m.Merge(p.State)
		log.Printf("Replicator: merged LWWMap[%s], keys=%d", msg.Key, len(m.Entries()))

	case "pncounter-gossip":
		var p PNCounterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		c := r.PNCounter(msg.Key)
		c.MergeSnapshot(p.PNCounterSnapshot)
		log.Printf("Replicator: merged PNCounter[%s], value=%d", msg.Key, c.Value())

	case "orflag-gossip":
		var snap ORSetSnapshot
		if err := json.Unmarshal(msg.Payload, &snap); err != nil {
			return err
		}
		f := r.ORFlag(msg.Key)
		f.MergeSnapshot(snap)
		log.Printf("Replicator: merged ORFlag[%s], value=%v", msg.Key, f.Value())

	case "lwwregister-gossip":
		var p LWWRegisterPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return err
		}
		reg := r.LWWRegister(msg.Key)
		reg.MergeSnapshot(p.LWWRegisterSnapshot)
		log.Printf("Replicator: merged LWWRegister[%s], value=%v", msg.Key, func() any { v, _ := reg.Get(); return v }())

	default:
		if r.OnUnknownKey != nil {
			r.OnUnknownKey(msg.Key, msg)
		}
	}
	return nil
}

func (r *Replicator) gossipAll(ctx context.Context) {
	r.mu.RLock()
	counters := make(map[string]*GCounter, len(r.counters))
	for k, v := range r.counters {
		counters[k] = v
	}
	sets := make(map[string]*ORSet, len(r.sets))
	for k, v := range r.sets {
		sets[k] = v
	}
	maps := make(map[string]*LWWMap, len(r.maps))
	for k, v := range r.maps {
		maps[k] = v
	}
	pnCounters := make(map[string]*PNCounter, len(r.pnCounters))
	for k, v := range r.pnCounters {
		pnCounters[k] = v
	}
	orFlags := make(map[string]*ORFlag, len(r.orFlags))
	for k, v := range r.orFlags {
		orFlags[k] = v
	}
	registers := make(map[string]*LWWRegister, len(r.registers))
	for k, v := range r.registers {
		registers[k] = v
	}
	r.mu.RUnlock()

	for key, c := range counters {
		r.gossipCounter(ctx, key, c)
	}
	for key, s := range sets {
		r.gossipSet(ctx, key, s)
	}
	for key, m := range maps {
		r.gossipMap(ctx, key, m)
	}
	for key, c := range pnCounters {
		r.gossipPNCounter(ctx, key, c)
	}
	for key, f := range orFlags {
		r.gossipORFlag(ctx, key, f)
	}
	for key, reg := range registers {
		r.gossipLWWRegister(ctx, key, reg)
	}
}

func (r *Replicator) gossipCounter(ctx context.Context, key string, c *GCounter) {
	payload, err := json.Marshal(GCounterPayload{State: c.Snapshot()})
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "gcounter-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipSet(ctx context.Context, key string, s *ORSet) {
	snap := s.Snapshot()
	payload, err := json.Marshal(snap)
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "orset-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipMap(ctx context.Context, key string, m *LWWMap) {
	payload, err := json.Marshal(LWWMapPayload{State: m.Snapshot()})
	if err != nil {
		return
	}
	msg := ReplicatorMsg{Type: "lwwmap-gossip", Key: key, Payload: payload}
	r.sendToPeers(ctx, msg)
}

func (r *Replicator) gossipPNCounter(ctx context.Context, key string, c *PNCounter) {
	payload, err := json.Marshal(PNCounterPayload{c.Snapshot()})
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "pncounter-gossip", Key: key, Payload: payload})
}

func (r *Replicator) gossipORFlag(ctx context.Context, key string, f *ORFlag) {
	payload, err := json.Marshal(f.Snapshot())
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "orflag-gossip", Key: key, Payload: payload})
}

func (r *Replicator) gossipLWWRegister(ctx context.Context, key string, reg *LWWRegister) {
	payload, err := json.Marshal(LWWRegisterPayload{reg.Snapshot()})
	if err != nil {
		return
	}
	r.sendToPeers(ctx, ReplicatorMsg{Type: "lwwregister-gossip", Key: key, Payload: payload})
}

func (r *Replicator) sendToPeers(ctx context.Context, msg ReplicatorMsg) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Replicator: marshal error: %v", err)
		return
	}
	r.mu.RLock()
	peers := append([]peerInfo(nil), r.peers...)
	r.mu.RUnlock()

	for _, p := range peers {
		if err := r.router.Send(ctx, p.path, data); err != nil {
			log.Printf("Replicator: send to %s failed: %v", p.path, err)
		}
	}
}

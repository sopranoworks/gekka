/*
 * replicator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package crdt

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
	Type    string          `json:"type"` // "gcounter-gossip", "orset-gossip", "get-reply"
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
	counters map[string]*GCounter
	sets     map[string]*ORSet

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
	r.mu.RUnlock()

	for key, c := range counters {
		r.gossipCounter(ctx, key, c)
	}
	for key, s := range sets {
		r.gossipSet(ctx, key, s)
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

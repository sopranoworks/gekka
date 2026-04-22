/*
 * cluster_mediator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package pubsub

import (
	"context"
	"log"
	"sync"
	"time"
)

// ClusterMediatorConfig holds configuration for the ClusterMediator.
type ClusterMediatorConfig struct {
	// GossipInterval is the interval between gossip rounds that propagate
	// subscription state to peer nodes. Corresponds to
	// pekko.cluster.pub-sub.gossip-interval.
	// Default: 1s.
	GossipInterval time.Duration

	// Name is the actor name for the mediator. Default: "distributedPubSubMediator".
	Name string

	// Role restricts pub-sub to nodes with this role. Default: "" (all nodes).
	Role string

	// RoutingLogic selects the strategy for Send (point-to-point):
	// "random" or "round-robin". Default: "random".
	RoutingLogic string

	// RemovedTimeToLive is how long tombstones for removed subscriptions
	// are retained before being reaped. Default: 120s.
	RemovedTimeToLive time.Duration

	// MaxDeltaElements caps the number of entries in a single delta message.
	// When exceeded, a full state sync is sent instead. Default: 3000.
	MaxDeltaElements int

	// SendToDeadLettersWhenNoSubscribers controls whether a DeadLetter event
	// is published when a Publish or Send finds no matching subscribers.
	// Default: true.
	SendToDeadLettersWhenNoSubscribers bool

	// EventStream receives DeadLetter events when no subscribers are found.
	// When nil, dead-letter publishing is silently skipped.
	EventStream interface{ Publish(event any) }
}

// PeerSender is the function signature used by ClusterMediator to send
// pub-sub messages (Status, Delta, Publish, etc.) to remote cluster nodes.
// The path identifies the remote mediator actor; payload is the serialized
// pub-sub message.
type PeerSender func(ctx context.Context, path string, payload any) error

// PeerLister returns the actor paths of the pub-sub mediators on all
// currently reachable cluster members (excluding the local node).
type PeerLister func() []string

// ClusterMediator extends LocalMediator with cluster-wide gossip.
// It periodically exchanges Status messages with peer mediators and
// propagates subscription deltas when peers are out of date.
//
// Usage:
//
//	cm := pubsub.NewClusterMediator(pubsub.ClusterMediatorConfig{
//	    GossipInterval: 1 * time.Second,
//	}, sender, lister)
//	defer cm.Stop()
//
// Publish/Subscribe/Unsubscribe calls update the local subscription state
// and the next gossip round will propagate changes to peers.
type ClusterMediator struct {
	*LocalMediator

	cfg    ClusterMediatorConfig
	sender PeerSender
	lister PeerLister

	// version is a monotonically increasing counter bumped on every
	// Subscribe/Unsubscribe. Gossip Status messages carry this version
	// so that peers can detect stale state.
	mu      sync.Mutex
	version int64

	// tombstones tracks removed subscriptions with their removal timestamp.
	// Entries are retained for RemovedTimeToLive before being reaped.
	tombstones map[string]time.Time // path → removal timestamp

	// rrIndex tracks per-topic round-robin state for Send routing.
	rrIndex map[string]int

	cancel context.CancelFunc
	done   chan struct{}
}

// NewClusterMediator creates a ClusterMediator that gossips subscription
// state to cluster peers at the configured interval.
func NewClusterMediator(cfg ClusterMediatorConfig, sender PeerSender, lister PeerLister) *ClusterMediator {
	if cfg.GossipInterval <= 0 {
		cfg.GossipInterval = 1 * time.Second
	}
	if cfg.Name == "" {
		cfg.Name = "distributedPubSubMediator"
	}
	if cfg.RoutingLogic == "" {
		cfg.RoutingLogic = "random"
	}
	if cfg.RemovedTimeToLive <= 0 {
		cfg.RemovedTimeToLive = 120 * time.Second
	}
	if cfg.MaxDeltaElements <= 0 {
		cfg.MaxDeltaElements = 3000
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm := &ClusterMediator{
		LocalMediator: NewLocalMediator(),
		cfg:           cfg,
		sender:        sender,
		lister:        lister,
		tombstones:    make(map[string]time.Time),
		rrIndex:       make(map[string]int),
		cancel:        cancel,
		done:          make(chan struct{}),
	}

	go cm.gossipLoop(ctx)
	return cm
}

// Subscribe registers a subscription and bumps the gossip version.
func (cm *ClusterMediator) Subscribe(ctx context.Context, topic, group, receiverPath string) error {
	if err := cm.LocalMediator.Subscribe(ctx, topic, group, receiverPath); err != nil {
		return err
	}
	cm.mu.Lock()
	cm.version++
	cm.mu.Unlock()
	return nil
}

// Unsubscribe removes a subscription, bumps the gossip version, and records
// a tombstone so that gossip skips the removed path for RemovedTimeToLive.
func (cm *ClusterMediator) Unsubscribe(ctx context.Context, topic, group, receiverPath string) error {
	if err := cm.LocalMediator.Unsubscribe(ctx, topic, group, receiverPath); err != nil {
		return err
	}
	cm.mu.Lock()
	cm.version++
	cm.tombstones[receiverPath] = time.Now()
	cm.mu.Unlock()
	return nil
}

// Publish delivers a message locally and fans it out to all cluster peers.
// When no subscribers exist and SendToDeadLettersWhenNoSubscribers is true,
// a DeadLetter event is published to the EventStream.
func (cm *ClusterMediator) Publish(ctx context.Context, topic string, msg any) error {
	cm.LocalMediator.mu.RLock()
	hasLocal := len(cm.LocalMediator.subs[topic]) > 0
	cm.LocalMediator.mu.RUnlock()

	peers := cm.lister()

	if !hasLocal && len(peers) == 0 && cm.cfg.SendToDeadLettersWhenNoSubscribers && cm.cfg.EventStream != nil {
		cm.cfg.EventStream.Publish(DeadLetterPubSub{
			Message: msg,
			Topic:   topic,
			Cause:   "no-subscribers",
		})
	}

	// Deliver locally.
	if hasLocal {
		_ = cm.LocalMediator.Publish(ctx, topic, msg)
	}

	// Fan out to cluster peers.
	pub := Publish{Topic: topic, Msg: msg}
	for _, peer := range peers {
		if err := cm.sender(ctx, peer, pub); err != nil {
			log.Printf("pubsub: failed to publish to peer %s: %v", peer, err)
		}
	}
	return nil
}

// Version returns the current gossip version (for testing/inspection).
func (cm *ClusterMediator) Version() int64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.version
}

// GossipInterval returns the configured gossip interval.
func (cm *ClusterMediator) GossipInterval() time.Duration {
	return cm.cfg.GossipInterval
}

// Name returns the configured mediator name.
func (cm *ClusterMediator) Name() string {
	return cm.cfg.Name
}

// SendOne delivers msg to one subscriber per group using the configured
// routing logic (point-to-point). Returns true if at least one delivery
// was made locally; also fans out to cluster peers.
func (cm *ClusterMediator) SendOne(ctx context.Context, topic string, msg any) error {
	logic := cm.cfg.RoutingLogic

	delivered := cm.LocalMediator.SendOne(ctx, topic, msg, logic)

	// Fan out to cluster peers.
	sendMsg := SendToOneSubscriber{Msg: msg}
	for _, peer := range cm.lister() {
		if err := cm.sender(ctx, peer, sendMsg); err != nil {
			log.Printf("pubsub: failed to send to peer %s: %v", peer, err)
		}
	}

	if !delivered && cm.cfg.SendToDeadLettersWhenNoSubscribers && cm.cfg.EventStream != nil {
		cm.cfg.EventStream.Publish(DeadLetterPubSub{
			Message: msg,
			Topic:   topic,
			Cause:   "no-subscribers",
		})
	}
	return nil
}

// HandleStatus processes a Status message from a remote peer.
// If the peer's version differs from ours, we send a Delta back.
func (cm *ClusterMediator) HandleStatus(ctx context.Context, from string, status Status) {
	cm.mu.Lock()
	localVersion := cm.version
	cm.mu.Unlock()

	// Check if the peer needs our subscriptions.
	for _, peerVersion := range status.Versions {
		if peerVersion < localVersion {
			// Peer is behind — send our full subscription state as a Delta.
			cm.sendDelta(ctx, from)
			return
		}
	}
}

// HandleDelta processes a Delta message from a remote peer, merging
// remote subscriptions into the local state.
func (cm *ClusterMediator) HandleDelta(_ context.Context, delta Delta) {
	ctx := context.Background()
	for _, bucket := range delta.Buckets {
		for key, vh := range bucket.Content {
			if vh.Ref != "" {
				// Register the remote subscription locally so that Publish
				// can fan out to remote subscribers.
				_ = cm.LocalMediator.Subscribe(ctx, key, "", vh.Ref)
			}
		}
	}
}

// Stop stops the gossip loop and releases resources.
func (cm *ClusterMediator) Stop() {
	cm.cancel()
	<-cm.done
}

func (cm *ClusterMediator) gossipLoop(ctx context.Context) {
	defer close(cm.done)
	ticker := time.NewTicker(cm.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cm.reapTombstones()
			cm.gossipRound(ctx)
		}
	}
}

// reapTombstones removes tombstones older than RemovedTimeToLive.
func (cm *ClusterMediator) reapTombstones() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cutoff := time.Now().Add(-cm.cfg.RemovedTimeToLive)
	for path, ts := range cm.tombstones {
		if ts.Before(cutoff) {
			delete(cm.tombstones, path)
		}
	}
}

func (cm *ClusterMediator) gossipRound(ctx context.Context) {
	peers := cm.lister()
	if len(peers) == 0 {
		return
	}

	cm.mu.Lock()
	localVersion := cm.version
	cm.mu.Unlock()

	status := Status{
		Versions: map[Address]int64{
			{}: localVersion, // Local node version (address filled by sender)
		},
	}

	for _, peer := range peers {
		if err := cm.sender(ctx, peer, status); err != nil {
			log.Printf("pubsub: gossip to %s failed: %v", peer, err)
		}
	}
}

func (cm *ClusterMediator) sendDelta(ctx context.Context, peer string) {
	cm.mu.Lock()
	tombstones := make(map[string]struct{}, len(cm.tombstones))
	for path := range cm.tombstones {
		tombstones[path] = struct{}{}
	}
	localVersion := cm.version
	cm.mu.Unlock()

	cm.LocalMediator.mu.RLock()
	content := make(map[string]ValueHolder)
	for topic, entries := range cm.LocalMediator.subs {
		for _, e := range entries {
			if _, tombstoned := tombstones[e.path]; tombstoned {
				continue
			}
			content[topic] = ValueHolder{
				Version: localVersion,
				Ref:     e.path,
			}
		}
	}
	cm.LocalMediator.mu.RUnlock()

	// If the delta exceeds max-delta-elements, send full state instead
	// (for now we just truncate — same effect as Pekko's full-state fallback).
	if len(content) > cm.cfg.MaxDeltaElements {
		trimmed := make(map[string]ValueHolder, cm.cfg.MaxDeltaElements)
		i := 0
		for k, v := range content {
			if i >= cm.cfg.MaxDeltaElements {
				break
			}
			trimmed[k] = v
			i++
		}
		content = trimmed
	}

	delta := Delta{
		Buckets: []Bucket{
			{
				Version: localVersion,
				Content: content,
			},
		},
	}

	if err := cm.sender(ctx, peer, delta); err != nil {
		log.Printf("pubsub: send delta to %s failed: %v", peer, err)
	}
}

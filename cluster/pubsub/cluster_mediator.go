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

	cancel context.CancelFunc
	done   chan struct{}
}

// NewClusterMediator creates a ClusterMediator that gossips subscription
// state to cluster peers at the configured interval.
func NewClusterMediator(cfg ClusterMediatorConfig, sender PeerSender, lister PeerLister) *ClusterMediator {
	if cfg.GossipInterval <= 0 {
		cfg.GossipInterval = 1 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm := &ClusterMediator{
		LocalMediator: NewLocalMediator(),
		cfg:           cfg,
		sender:        sender,
		lister:        lister,
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

// Unsubscribe removes a subscription and bumps the gossip version.
func (cm *ClusterMediator) Unsubscribe(ctx context.Context, topic, group, receiverPath string) error {
	if err := cm.LocalMediator.Unsubscribe(ctx, topic, group, receiverPath); err != nil {
		return err
	}
	cm.mu.Lock()
	cm.version++
	cm.mu.Unlock()
	return nil
}

// Publish delivers a message locally and fans it out to all cluster peers.
func (cm *ClusterMediator) Publish(ctx context.Context, topic string, msg any) error {
	// Deliver locally first.
	if err := cm.LocalMediator.Publish(ctx, topic, msg); err != nil {
		return err
	}

	// Fan out to cluster peers.
	pub := Publish{Topic: topic, Msg: msg}
	for _, peer := range cm.lister() {
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
			cm.gossipRound(ctx)
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
	cm.LocalMediator.mu.RLock()
	content := make(map[string]ValueHolder)
	for topic, entries := range cm.LocalMediator.subs {
		for _, e := range entries {
			content[topic] = ValueHolder{
				Version: cm.version,
				Ref:     e.path,
			}
		}
	}
	cm.LocalMediator.mu.RUnlock()

	delta := Delta{
		Buckets: []Bucket{
			{
				Version: cm.version,
				Content: content,
			},
		},
	}

	if err := cm.sender(ctx, peer, delta); err != nil {
		log.Printf("pubsub: send delta to %s failed: %v", peer, err)
	}
}

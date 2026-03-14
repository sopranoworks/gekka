/*
 * cluster_singleton_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"log"

	"github.com/sopranoworks/gekka/actor"
)

// ClusterSingletonManager hosts a singleton actor on the oldest cluster node.
//
// It listens to cluster membership events and ensures exactly one instance of
// the singleton actor is alive at any given time — always on the oldest Up
// member (the one with the lowest upNumber, matching Pekko's selection rule).
//
// Usage:
//
//	mgr := cluster.NewClusterSingletonManager(cm, actor.Props{
//	    New: func() actor.Actor { return &MyActor{} },
//	}, "")
//	ref, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor { return mgr }}, "singletonManager")
//
// Pekko names the singleton child "singleton" by default; this manager does
// the same, so a ClusterSingletonProxy at "/user/singletonManager" will route
// to "/user/singletonManager/singleton".
type ClusterSingletonManager struct {
	actor.BaseActor

	cm             *ClusterManager
	role           string       // optional role filter; empty = any node
	singletonProps actor.Props  // factory for the singleton actor
	singletonRef   actor.Ref    // non-nil when the singleton is running on this node
}

// NewClusterSingletonManager creates a manager actor that will spawn/stop the
// singleton as cluster leadership changes.
//
// cm is the cluster manager used for membership queries and event subscriptions.
// singletonProps provides the factory for the singleton actor.
// role restricts the set of eligible nodes; pass "" to consider all Up nodes.
func NewClusterSingletonManager(cm *ClusterManager, singletonProps actor.Props, role string) *ClusterSingletonManager {
	return &ClusterSingletonManager{
		BaseActor:      actor.NewBaseActor(),
		cm:             cm,
		role:           role,
		singletonProps: singletonProps,
	}
}

// PreStart subscribes to cluster events and starts the singleton if this node
// is already the oldest at startup time.
func (m *ClusterSingletonManager) PreStart() {
	m.cm.Subscribe(m.Self(),
		EventMemberUp,
		EventMemberLeft,
		EventMemberExited,
		EventMemberRemoved,
	)
	m.maybeSpawnOrStop()
}

// PostStop unsubscribes from cluster events and stops the singleton actor if
// it is still running locally.
func (m *ClusterSingletonManager) PostStop() {
	m.cm.Unsubscribe(m.Self())
	if m.singletonRef != nil {
		log.Printf("ClusterSingletonManager: stopping singleton on shutdown")
		m.System().Stop(m.singletonRef)
		m.singletonRef = nil
	}
}

// Receive dispatches cluster domain events and monitors singleton lifecycle.
func (m *ClusterSingletonManager) Receive(msg any) {
	switch msg.(type) {
	case MemberUp, MemberLeft, MemberExited, MemberRemoved:
		m.maybeSpawnOrStop()
	default:
		// Forward unrecognised messages to the singleton if it is running.
		if m.singletonRef != nil {
			m.singletonRef.Tell(msg, m.Sender())
		}
	}
}

// isLocalOldest returns true when the local node is the oldest Up/WeaklyUp
// member eligible to host the singleton.
func (m *ClusterSingletonManager) isLocalOldest() bool {
	ua := m.cm.OldestNode(m.role)
	if ua == nil {
		return false
	}
	localAddr := m.cm.LocalAddress.GetAddress()
	oldest := ua.GetAddress()
	return oldest.GetHostname() == localAddr.GetHostname() &&
		oldest.GetPort() == localAddr.GetPort()
}

// maybeSpawnOrStop starts the singleton when this node becomes the oldest, and
// stops it when leadership transfers to another node.
func (m *ClusterSingletonManager) maybeSpawnOrStop() {
	if m.isLocalOldest() {
		if m.singletonRef == nil {
			ref, err := m.System().ActorOf(m.singletonProps, "singleton")
			if err != nil {
				log.Printf("ClusterSingletonManager: failed to spawn singleton: %v", err)
				return
			}
			m.singletonRef = ref
			m.System().Watch(m.Self(), ref)
			log.Printf("ClusterSingletonManager: singleton spawned at %s", ref.Path())
		}
	} else {
		if m.singletonRef != nil {
			log.Printf("ClusterSingletonManager: stopping singleton — no longer oldest node")
			m.System().Stop(m.singletonRef)
			m.singletonRef = nil
		}
	}
}

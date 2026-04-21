/*
 * cluster_singleton_manager.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"context"
	"log"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
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

	cm             *cluster.ClusterManager
	role           string      // optional role filter; empty = any node
	dataCenter     string      // optional DC filter; empty = any DC
	singletonProps actor.Props // factory for the singleton actor
	singletonRef   actor.Ref   // non-nil when the singleton is running on this node

	// HandOverRetryInterval is how often the manager retries handover
	// coordination during leadership transfer. Default: 1s.
	handOverRetryInterval time.Duration

	// Lease-based coordination (optional). When set, the manager must acquire
	// the lease before starting the singleton and releases it on handoff/stop.
	lease            icluster.Lease
	leaseRetryDelay  time.Duration // backoff between retries; default 5s
	leaseHeld        bool
}

// NewClusterSingletonManager creates a manager actor that will spawn/stop the
// singleton as cluster leadership changes.
//
// cm is the cluster manager used for membership queries and event subscriptions.
// singletonProps provides the factory for the singleton actor.
// role restricts the set of eligible nodes; pass "" to consider all Up nodes.
func NewClusterSingletonManager(cm *cluster.ClusterManager, singletonProps actor.Props, role string) *ClusterSingletonManager {
	return &ClusterSingletonManager{
		BaseActor:             actor.NewBaseActor(),
		cm:                    cm,
		role:                  role,
		singletonProps:        singletonProps,
		handOverRetryInterval: 1 * time.Second,
	}
}

// WithHandOverRetryInterval sets how often the manager retries handover
// coordination during leadership transfer.
func (m *ClusterSingletonManager) WithHandOverRetryInterval(d time.Duration) *ClusterSingletonManager {
	if d > 0 {
		m.handOverRetryInterval = d
	}
	return m
}

// Role returns the configured role filter.
func (m *ClusterSingletonManager) Role() string { return m.role }

// HandOverRetryInterval returns the configured hand-over retry interval.
func (m *ClusterSingletonManager) HandOverRetryInterval() time.Duration { return m.handOverRetryInterval }

// WithDataCenter restricts this manager to host the singleton only when this
// node is the oldest member of the given data center.
func (m *ClusterSingletonManager) WithDataCenter(dc string) cluster.ClusterSingletonManagerInterface {
	m.dataCenter = dc
	return m
}

// WithLease configures the manager to acquire a distributed lease before
// starting the singleton actor. This prevents split-brain dual-singletons.
func (m *ClusterSingletonManager) WithLease(lease icluster.Lease) *ClusterSingletonManager {
	m.lease = lease
	return m
}

// WithLeaseRetryDelay sets the backoff delay between lease acquisition retries.
// Default is 5 seconds.
func (m *ClusterSingletonManager) WithLeaseRetryDelay(d time.Duration) *ClusterSingletonManager {
	m.leaseRetryDelay = d
	return m
}

// PreStart subscribes to cluster events and starts the singleton if this node
// is already the oldest at startup time.
func (m *ClusterSingletonManager) PreStart() {
	m.cm.Subscribe(m.Self(),
		cluster.EventMemberUp,
		cluster.EventMemberLeft,
		cluster.EventMemberExited,
		cluster.EventMemberRemoved,
	)
	m.maybeSpawnOrStop()
}

// PostStop unsubscribes from cluster events and stops the singleton actor if
// it is still running locally, releasing any held lease.
func (m *ClusterSingletonManager) PostStop() {
	m.cm.Unsubscribe(m.Self())
	if m.singletonRef != nil {
		log.Printf("ClusterSingletonManager: stopping singleton on shutdown")
		m.System().Stop(m.singletonRef)
		m.singletonRef = nil
	}
	m.releaseLease()
}

// Receive dispatches cluster domain events and monitors singleton lifecycle.
func (m *ClusterSingletonManager) Receive(msg any) {
	switch msg.(type) {
	case cluster.MemberUp, cluster.MemberLeft, cluster.MemberExited, cluster.MemberRemoved:
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
	var ua *gproto_cluster.UniqueAddress
	if m.dataCenter != "" {
		ua = m.cm.OldestNodeInDC(m.dataCenter, m.role)
	} else {
		ua = m.cm.OldestNode(m.role)
	}
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
			if !m.acquireLease() {
				return
			}
			ref, err := m.System().ActorOf(m.singletonProps, "singleton")
			if err != nil {
				log.Printf("ClusterSingletonManager: failed to spawn singleton: %v", err)
				m.releaseLease()
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
			m.releaseLease()
		}
	}
}

// acquireLease attempts to acquire the configured lease. Returns true if no
// lease is configured or if the lease was acquired successfully. On failure
// it retries once after leaseRetryDelay.
func (m *ClusterSingletonManager) acquireLease() bool {
	if m.lease == nil {
		return true
	}
	if m.leaseHeld {
		return true
	}

	ctx := context.Background()
	ok, err := m.lease.Acquire(ctx, func(err error) {
		log.Printf("ClusterSingletonManager: lease lost: %v", err)
		// On lease loss, stop the singleton.
		if m.singletonRef != nil {
			m.System().Stop(m.singletonRef)
			m.singletonRef = nil
		}
		m.leaseHeld = false
	})
	if err != nil || !ok {
		retryDelay := m.leaseRetryDelay
		if retryDelay == 0 {
			retryDelay = m.handOverRetryInterval
		}
		log.Printf("ClusterSingletonManager: lease acquisition failed, retrying in %s", retryDelay)
		time.Sleep(retryDelay)
		ok, err = m.lease.Acquire(ctx, nil)
		if err != nil || !ok {
			log.Printf("ClusterSingletonManager: lease acquisition retry failed: %v", err)
			return false
		}
	}
	m.leaseHeld = true
	return true
}

// releaseLease releases the held lease if any.
func (m *ClusterSingletonManager) releaseLease() {
	if m.lease == nil || !m.leaseHeld {
		return
	}
	if _, err := m.lease.Release(context.Background()); err != nil {
		log.Printf("ClusterSingletonManager: lease release failed: %v", err)
	}
	m.leaseHeld = false
}

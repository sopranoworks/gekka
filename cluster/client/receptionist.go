/*
 * cluster/client/receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// ── Internal receptionist messages ───────────────────────────────────────────

// registerService is an internal message used by the receptionist to track
// actors that have been registered as reachable services.
type registerService struct {
	path string
	ref  actor.Ref
}

// unregisterService removes a previously registered service path.
type unregisterService struct {
	path string
}

// checkClientHeartbeats is a periodic internal tick used by the receptionist
// to evict clients that have stopped sending heartbeats.
type checkClientHeartbeats struct{}

// ── ClusterReceptionist ───────────────────────────────────────────────────────

// clientEntry tracks an active ClusterClient connection.
type clientEntry struct {
	senderPath   string
	lastHeartbeat time.Time
}

// ClusterReceptionist is deployed on cluster member nodes to act as a gateway
// for external ClusterClient actors.  It:
//
//  1. Answers GetContacts requests with a list of active receptionist paths.
//  2. Forwards Send / SendToAll / Publish envelopes to local or cluster-wide
//     registered actors.
//  3. Maintains heartbeat liveness tracking for connected clients.
//
// Spawn the receptionist via node.System.ActorOf:
//
//	rec := client.NewClusterReceptionist(cm, cfg)
//	node.System.ActorOf(actor.Props{New: func() actor.Actor { return rec }},
//	    cfg.Name) // typically "receptionist"
type ClusterReceptionist struct {
	actor.BaseActor

	cm     *cluster.ClusterManager
	cfg    ReceptionistConfig
	router cluster.Router

	// services holds paths registered as reachable services (path → ref).
	servicesMu sync.RWMutex
	services   map[string]actor.Ref

	// clients tracks heartbeat liveness for each known ClusterClient.
	clientsMu sync.RWMutex
	clients   map[string]*clientEntry // keyed by sender path

	stopCheck chan struct{}
}

// NewClusterReceptionist creates a ClusterReceptionist using the supplied
// cluster manager and configuration.  router is used to forward unwrapped
// user messages to registered services.
func NewClusterReceptionist(cm *cluster.ClusterManager, cfg ReceptionistConfig, router cluster.Router) *ClusterReceptionist {
	if cfg.Name == "" {
		cfg.Name = "receptionist"
	}
	if cfg.NumberOfContacts == 0 {
		cfg.NumberOfContacts = 3
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.AcceptableHeartbeatPause == 0 {
		cfg.AcceptableHeartbeatPause = 13 * time.Second
	}
	return &ClusterReceptionist{
		BaseActor: actor.NewBaseActor(),
		cm:        cm,
		cfg:       cfg,
		router:    router,
		services:  make(map[string]actor.Ref),
		clients:   make(map[string]*clientEntry),
		stopCheck: make(chan struct{}),
	}
}

// PreStart starts the periodic client-heartbeat checker.
func (r *ClusterReceptionist) PreStart() {
	slog.Info("ClusterReceptionist: starting", "name", r.cfg.Name, "role", r.cfg.Role)
	go r.heartbeatChecker()
}

// PostStop halts the heartbeat checker goroutine.
func (r *ClusterReceptionist) PostStop() {
	close(r.stopCheck)
	slog.Info("ClusterReceptionist: stopped")
}

// heartbeatChecker runs in a background goroutine and periodically evicts
// clients that have not sent a heartbeat within the acceptable pause window.
func (r *ClusterReceptionist) heartbeatChecker() {
	tick := time.NewTicker(r.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-r.stopCheck:
			return
		case <-tick.C:
			r.Self().Tell(checkClientHeartbeats{})
		}
	}
}

// Receive dispatches messages delivered to the receptionist.
func (r *ClusterReceptionist) Receive(msg any) {
	switch m := msg.(type) {
	case Heartbeat:
		r.handleHeartbeat()

	case GetContacts:
		r.handleGetContacts()

	case Send:
		r.handleSend(m)

	case SendToAll:
		r.handleSendToAll(m)

	case Publish:
		r.handlePublish(m)

	case registerService:
		r.servicesMu.Lock()
		r.services[m.path] = m.ref
		r.servicesMu.Unlock()
		slog.Debug("ClusterReceptionist: registered service", "path", m.path)

	case unregisterService:
		r.servicesMu.Lock()
		delete(r.services, m.path)
		r.servicesMu.Unlock()
		slog.Debug("ClusterReceptionist: unregistered service", "path", m.path)

	case checkClientHeartbeats:
		r.evictStaleClients()

	default:
		slog.Debug("ClusterReceptionist: unknown message", "type", fmt.Sprintf("%T", msg))
	}
}

// handleHeartbeat records a liveness ping from a connected client.
func (r *ClusterReceptionist) handleHeartbeat() {
	sender := r.Sender()
	if sender == nil {
		return
	}
	senderPath := sender.Path()
	r.clientsMu.Lock()
	if e, ok := r.clients[senderPath]; ok {
		e.lastHeartbeat = time.Now()
	} else {
		r.clients[senderPath] = &clientEntry{
			senderPath:   senderPath,
			lastHeartbeat: time.Now(),
		}
		slog.Debug("ClusterReceptionist: new client registered", "path", senderPath)
	}
	r.clientsMu.Unlock()

	// Reply with HeartbeatRsp.
	sender.Tell(HeartbeatRsp{}, r.Self())
}

// handleGetContacts replies with a Contacts message containing up to
// cfg.NumberOfContacts active receptionist paths from cluster members.
func (r *ClusterReceptionist) handleGetContacts() {
	sender := r.Sender()
	if sender == nil {
		return
	}

	paths := r.collectContactPaths()
	sender.Tell(Contacts{Paths: paths}, r.Self())
}

// collectContactPaths builds a list of receptionist actor paths for Up cluster
// members, capped at cfg.NumberOfContacts.
func (r *ClusterReceptionist) collectContactPaths() []string {
	state := r.cm.GetState()
	localAddr := r.cm.GetLocalAddress().GetAddress()
	proto := r.cm.Proto()

	var paths []string
	for _, m := range state.Members {
		if len(paths) >= r.cfg.NumberOfContacts {
			break
		}
		ua := state.AllAddresses[m.GetAddressIndex()]
		a := ua.GetAddress()
		paths = append(paths, fmt.Sprintf("%s://%s@%s:%d/system/%s",
			proto,
			localAddr.GetSystem(),
			a.GetHostname(),
			a.GetPort(),
			r.cfg.Name,
		))
	}
	return paths
}

// handleSend forwards a Send envelope to the registered service at m.Path,
// preferring a local ref when LocalAffinity is set.
func (r *ClusterReceptionist) handleSend(m Send) {
	r.servicesMu.RLock()
	ref, ok := r.services[m.Path]
	r.servicesMu.RUnlock()

	if ok {
		ref.Tell(m.Msg, r.Sender())
		return
	}

	// Fall back to remote delivery via the router.
	if r.router != nil {
		localAddr := r.cm.GetLocalAddress().GetAddress()
		proto := r.cm.Proto()
		fullPath := fmt.Sprintf("%s://%s@%s:%d%s",
			proto,
			localAddr.GetSystem(),
			localAddr.GetHostname(),
			localAddr.GetPort(),
			m.Path,
		)
		if err := r.router.Send(context.TODO(), fullPath, m.Msg); err != nil {
			slog.Warn("ClusterReceptionist: Send failed", "path", m.Path, "err", err)
		}
	}
}

// handleSendToAll broadcasts a SendToAll message to every registered service
// whose path matches m.Path, across the cluster via the router.
func (r *ClusterReceptionist) handleSendToAll(m SendToAll) {
	r.servicesMu.RLock()
	ref, ok := r.services[m.Path]
	r.servicesMu.RUnlock()

	if ok {
		ref.Tell(m.Msg, r.Sender())
	}

	// Broadcast to cluster members via router.
	if r.router != nil {
		state := r.cm.GetState()
		localAddr := r.cm.GetLocalAddress().GetAddress()
		proto := r.cm.Proto()

		for _, ua := range state.AllAddresses {
			a := ua.GetAddress()
			if a.GetHostname() == localAddr.GetHostname() && a.GetPort() == localAddr.GetPort() {
				continue // already delivered locally above
			}
			fullPath := fmt.Sprintf("%s://%s@%s:%d%s",
				proto,
				localAddr.GetSystem(),
				a.GetHostname(),
				a.GetPort(),
				m.Path,
			)
			if err := r.router.Send(context.TODO(), fullPath, m.Msg); err != nil {
				slog.Debug("ClusterReceptionist: SendToAll node failed", "path", fullPath, "err", err)
			}
		}
	}
}

// handlePublish forwards a Publish envelope to the router (which integrates
// with the cluster pub/sub mediator when present).
func (r *ClusterReceptionist) handlePublish(m Publish) {
	if r.router == nil {
		return
	}
	localAddr := r.cm.GetLocalAddress().GetAddress()
	proto := r.cm.Proto()
	mediatorPath := fmt.Sprintf("%s://%s@%s:%d/system/distributedPubSubMediator",
		proto,
		localAddr.GetSystem(),
		localAddr.GetHostname(),
		localAddr.GetPort(),
	)
	if err := r.router.Send(context.TODO(), mediatorPath, m); err != nil {
		slog.Warn("ClusterReceptionist: Publish failed", "topic", m.Topic, "err", err)
	}
}

// evictStaleClients removes clients that have not sent a heartbeat within the
// acceptable pause window.
func (r *ClusterReceptionist) evictStaleClients() {
	deadline := time.Now().Add(-(r.cfg.HeartbeatInterval + r.cfg.AcceptableHeartbeatPause))
	r.clientsMu.Lock()
	defer r.clientsMu.Unlock()
	for path, e := range r.clients {
		if e.lastHeartbeat.Before(deadline) {
			slog.Info("ClusterReceptionist: evicting stale client", "path", path)
			delete(r.clients, path)
		}
	}
}

// RegisterService makes a local actor ref discoverable by external clients via
// the given relative path (e.g. "/user/myService").
func (r *ClusterReceptionist) RegisterService(path string, ref actor.Ref) {
	r.Self().Tell(registerService{path: path, ref: ref})
}

// UnregisterService removes a service registration.
func (r *ClusterReceptionist) UnregisterService(path string) {
	r.Self().Tell(unregisterService{path: path})
}

// ConnectedClients returns the paths of all currently-tracked clients.
// The snapshot is taken under the clientsMu read lock.
func (r *ClusterReceptionist) ConnectedClients() []string {
	r.clientsMu.RLock()
	defer r.clientsMu.RUnlock()
	paths := make([]string, 0, len(r.clients))
	for p := range r.clients {
		paths = append(paths, p)
	}
	return paths
}

/*
 * cluster/client/client.go
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

// ── Internal client messages ──────────────────────────────────────────────────

// heartbeatTick is a periodic internal tick that triggers the client to send
// a Heartbeat to the current contact point.
type heartbeatTick struct{}

// refreshContactsTick prompts the client to request an updated Contacts list.
type refreshContactsTick struct{}

// connectionDeadline is a periodic check that looks for heartbeat timeout.
type connectionDeadline struct{}

// ── ClusterClient ─────────────────────────────────────────────────────────────

// connectionState tracks the client's view of its current contact point.
type connectionState struct {
	contactPath   string    // Artery path to the receptionist
	lastHeartbeat time.Time // last HeartbeatRsp received
	connected     bool
}

// bufferedMsg holds a user message queued while the client is disconnected.
type bufferedMsg struct {
	msg     any
	deliver func(contactPath string) error
}

// ClusterClient allows an external (non-cluster-member) process to send
// messages to actors inside a Gekka cluster.  It:
//
//  1. Connects to a ClusterReceptionist using the configured initial contacts.
//  2. Automatically rotates to an alternative contact when the current one
//     fails (contact-point failover).
//  3. Buffers outgoing messages while reconnecting (up to cfg.BufferSize).
//  4. Sends periodic heartbeats and detects stale connections.
//
// Example usage:
//
//	cfg := client.DefaultConfig()
//	cfg.InitialContacts = []string{"pekko://ClusterSystem@host:2552/system/receptionist"}
//	cc := client.NewClusterClient(cfg, router)
//	// Spawn cc as an actor in the non-cluster node's ActorSystem.
//	ref, _ := system.ActorOf(actor.Props{New: func() actor.Actor { return cc }}, "clusterClient")
//
//	// Send to a registered service:
//	ref.Tell(client.Send{Path: "/user/myService", Msg: "hello"})
type ClusterClient struct {
	actor.BaseActor

	cfg    Config
	router cluster.Router

	// contacts is the currently-known list of receptionist paths.
	// Populated from initial-contacts and refreshed via GetContacts replies.
	contactsMu sync.RWMutex
	contacts   []string
	contactIdx int // round-robin index into contacts

	conn connectionState

	// buffer holds messages queued while not connected.
	bufMu  sync.Mutex
	buffer []bufferedMsg

	// stopTimers signals background goroutines to stop.
	stopTimers chan struct{}
}

// NewClusterClient creates a ClusterClient with the given configuration.
// router is the transport used to send messages to receptionist paths.
func NewClusterClient(cfg Config, router cluster.Router) *ClusterClient {
	if cfg.EstablishingGetContactsInterval == 0 {
		cfg.EstablishingGetContactsInterval = 3 * time.Second
	}
	if cfg.RefreshContactsInterval == 0 {
		cfg.RefreshContactsInterval = 60 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.AcceptableHeartbeatPause == 0 {
		cfg.AcceptableHeartbeatPause = 13 * time.Second
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 1000
	}
	contacts := make([]string, len(cfg.InitialContacts))
	copy(contacts, cfg.InitialContacts)
	return &ClusterClient{
		BaseActor:  actor.NewBaseActor(),
		cfg:        cfg,
		router:     router,
		contacts:   contacts,
		stopTimers: make(chan struct{}),
	}
}

// PreStart begins heartbeat and contact-refresh timers.
func (c *ClusterClient) PreStart() {
	slog.Info("ClusterClient: starting", "contacts", c.cfg.InitialContacts)
	c.tryConnect()
	go c.runTimers()
}

// PostStop halts all background goroutines.
func (c *ClusterClient) PostStop() {
	close(c.stopTimers)
	slog.Info("ClusterClient: stopped")
}

// runTimers drives the periodic heartbeat, contact-refresh, and deadline
// checks in a single background goroutine.
func (c *ClusterClient) runTimers() {
	heartbeat := time.NewTicker(c.cfg.HeartbeatInterval)
	deadline := time.NewTicker(c.cfg.HeartbeatInterval + c.cfg.AcceptableHeartbeatPause)
	refresh := time.NewTicker(c.cfg.RefreshContactsInterval)
	defer heartbeat.Stop()
	defer deadline.Stop()
	defer refresh.Stop()

	for {
		select {
		case <-c.stopTimers:
			return
		case <-heartbeat.C:
			c.Self().Tell(heartbeatTick{})
		case <-deadline.C:
			c.Self().Tell(connectionDeadline{})
		case <-refresh.C:
			c.Self().Tell(refreshContactsTick{})
		}
	}
}

// Receive dispatches messages to the appropriate handler.
func (c *ClusterClient) Receive(msg any) {
	switch m := msg.(type) {
	// ── User-facing outbound messages ────────────────────────────────────
	case Send:
		c.forward(func(contactPath string) error {
			return c.router.Send(context.TODO(), contactPath, m)
		}, msg)

	case SendToAll:
		c.forward(func(contactPath string) error {
			return c.router.Send(context.TODO(), contactPath, m)
		}, msg)

	case Publish:
		c.forward(func(contactPath string) error {
			return c.router.Send(context.TODO(), contactPath, m)
		}, msg)

	// ── Receptionist replies ─────────────────────────────────────────────
	case HeartbeatRsp:
		c.conn.lastHeartbeat = time.Now()
		slog.Debug("ClusterClient: heartbeat acknowledged", "contact", c.conn.contactPath)

	case Contacts:
		c.handleContacts(m)

	// ── Internal ticks ───────────────────────────────────────────────────
	case heartbeatTick:
		c.sendHeartbeat()

	case refreshContactsTick:
		c.sendGetContacts()

	case connectionDeadline:
		c.checkDeadline()

	default:
		slog.Debug("ClusterClient: unknown message", "type", fmt.Sprintf("%T", msg))
	}
}

// tryConnect attempts to reach the first available contact point by sending a
// GetContacts message and marking the connection as pending.
func (c *ClusterClient) tryConnect() {
	c.contactsMu.RLock()
	n := len(c.contacts)
	c.contactsMu.RUnlock()

	if n == 0 {
		slog.Warn("ClusterClient: no contact points configured")
		return
	}
	c.rotateContact()
	c.sendGetContacts()
}

// rotateContact advances contactIdx to the next available contact in a
// round-robin fashion and updates conn.contactPath.
func (c *ClusterClient) rotateContact() {
	c.contactsMu.RLock()
	n := len(c.contacts)
	if n == 0 {
		c.contactsMu.RUnlock()
		return
	}
	c.contactIdx = (c.contactIdx + 1) % n
	path := c.contacts[c.contactIdx]
	c.contactsMu.RUnlock()

	c.conn.contactPath = path
	c.conn.connected = false
	slog.Info("ClusterClient: using contact point", "path", path)
}

// sendHeartbeat sends a Heartbeat to the current contact point, if connected.
func (c *ClusterClient) sendHeartbeat() {
	if c.conn.contactPath == "" {
		c.tryConnect()
		return
	}
	if err := c.router.SendWithSender(context.TODO(), c.conn.contactPath, c.Self().Path(), Heartbeat{}); err != nil {
		slog.Debug("ClusterClient: heartbeat send failed", "contact", c.conn.contactPath, "err", err)
	}
}

// sendGetContacts asks the current contact point for an updated Contacts list.
func (c *ClusterClient) sendGetContacts() {
	if c.conn.contactPath == "" {
		return
	}
	if err := c.router.SendWithSender(context.TODO(), c.conn.contactPath, c.Self().Path(), GetContacts{}); err != nil {
		slog.Debug("ClusterClient: GetContacts send failed", "contact", c.conn.contactPath, "err", err)
	}
}

// handleContacts merges the received paths into the contacts list, marks the
// connection as established, and drains the buffer.
func (c *ClusterClient) handleContacts(m Contacts) {
	if len(m.Paths) > 0 {
		c.contactsMu.Lock()
		existing := make(map[string]struct{}, len(c.contacts))
		for _, p := range c.contacts {
			existing[p] = struct{}{}
		}
		for _, p := range m.Paths {
			if _, ok := existing[p]; !ok {
				c.contacts = append(c.contacts, p)
			}
		}
		c.contactsMu.Unlock()
	}
	if !c.conn.connected {
		c.conn.connected = true
		c.conn.lastHeartbeat = time.Now()
		slog.Info("ClusterClient: connected to cluster", "contact", c.conn.contactPath)
		c.drainBuffer()
	}
}

// checkDeadline switches to the next contact point when the heartbeat window
// has expired without a HeartbeatRsp.
func (c *ClusterClient) checkDeadline() {
	if !c.conn.connected {
		return
	}
	window := c.cfg.HeartbeatInterval + c.cfg.AcceptableHeartbeatPause
	if time.Since(c.conn.lastHeartbeat) > window {
		slog.Warn("ClusterClient: heartbeat deadline exceeded — rotating contact",
			"contact", c.conn.contactPath,
			"last", c.conn.lastHeartbeat,
		)
		c.conn.connected = false
		c.rotateContact()
		c.sendGetContacts()
	}
}

// forward either delivers the message immediately (if connected) or queues it
// in the buffer for later delivery once a contact is (re)established.
func (c *ClusterClient) forward(deliver func(contactPath string) error, msg any) {
	if c.conn.connected && c.conn.contactPath != "" {
		if err := deliver(c.conn.contactPath); err != nil {
			slog.Warn("ClusterClient: deliver failed, buffering", "err", err)
			c.bufferMsg(bufferedMsg{msg: msg, deliver: deliver})
		}
		return
	}
	c.bufferMsg(bufferedMsg{msg: msg, deliver: deliver})
}

// bufferMsg appends a message to the outbound buffer, dropping the oldest
// entry when the buffer is full.
func (c *ClusterClient) bufferMsg(bm bufferedMsg) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	if c.cfg.BufferSize <= 0 {
		return
	}
	if len(c.buffer) >= c.cfg.BufferSize {
		c.buffer = c.buffer[1:] // drop oldest
	}
	c.buffer = append(c.buffer, bm)
}

// drainBuffer attempts to deliver all buffered messages through the current
// contact point.  Messages that still fail are re-queued.
func (c *ClusterClient) drainBuffer() {
	c.bufMu.Lock()
	pending := c.buffer
	c.buffer = nil
	c.bufMu.Unlock()

	var retry []bufferedMsg
	for _, bm := range pending {
		if err := bm.deliver(c.conn.contactPath); err != nil {
			retry = append(retry, bm)
		}
	}
	if len(retry) > 0 {
		c.bufMu.Lock()
		c.buffer = append(retry, c.buffer...)
		if len(c.buffer) > c.cfg.BufferSize {
			c.buffer = c.buffer[len(c.buffer)-c.cfg.BufferSize:]
		}
		c.bufMu.Unlock()
	}
}

// IsConnected reports whether the client has an established connection.
func (c *ClusterClient) IsConnected() bool {
	return c.conn.connected
}

// CurrentContact returns the Artery path of the receptionist the client is
// currently using.
func (c *ClusterClient) CurrentContact() string {
	return c.conn.contactPath
}

// BufferedCount returns how many messages are queued waiting for reconnection.
func (c *ClusterClient) BufferedCount() int {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	return len(c.buffer)
}

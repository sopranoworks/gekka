/*
 * cluster_singleton_proxy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

// bufferedMessage holds a message that was sent while the singleton location
// was unknown. It will be delivered once the proxy resolves the singleton.
type bufferedMessage struct {
	ctx        context.Context
	msg        any
	senderPath string // empty for plain Send
}

// ClusterSingletonProxy routes messages to the singleton actor on the oldest cluster node.
// In Pekko, ClusterSingletonManager names the child actor "singleton" by default.
// So a manager at "/user/singletonManager" hosts the singleton at "/user/singletonManager/singleton".
type ClusterSingletonProxy struct {
	cm            *cluster.ClusterManager
	router        cluster.Router
	managerPath   string // relative actor path of the singleton manager, e.g. "/user/singletonManager"
	singletonName string // name of the singleton actor, defaults to "singleton"
	role          string // optional role filter; empty means any node
	dataCenter    string // optional DC filter; empty means any DC

	// identificationInterval is how often the proxy periodically re-resolves
	// the oldest node. Zero disables periodic re-resolution. Default: 1s.
	identificationInterval time.Duration

	// bufferSize is the maximum number of messages buffered when the singleton
	// location is unknown. Default: 1000.
	bufferSize int

	mu      sync.Mutex
	buffer  []bufferedMessage
	stopCh  chan struct{}
	started bool
}

func NewClusterSingletonProxy(cm *cluster.ClusterManager, router cluster.Router, managerPath, role string) *ClusterSingletonProxy {
	return &ClusterSingletonProxy{
		cm:                     cm,
		router:                 router,
		managerPath:            managerPath,
		singletonName:          "singleton",
		role:                   role,
		identificationInterval: 1 * time.Second,
		bufferSize:             1000,
		stopCh:                 make(chan struct{}),
	}
}

// IdentificationInterval returns the current singleton identification interval.
func (p *ClusterSingletonProxy) IdentificationInterval() time.Duration {
	return p.identificationInterval
}

// BufferSizeLimit returns the configured maximum buffer size.
func (p *ClusterSingletonProxy) BufferSizeLimit() int {
	return p.bufferSize
}

// WithIdentificationInterval sets how often the proxy periodically re-resolves
// the oldest node to identify the singleton location.
func (p *ClusterSingletonProxy) WithIdentificationInterval(d time.Duration) *ClusterSingletonProxy {
	if d > 0 {
		p.identificationInterval = d
	}
	return p
}

// WithBufferSize sets the maximum number of messages buffered when the
// singleton location is unknown.
func (p *ClusterSingletonProxy) WithBufferSize(size int) *ClusterSingletonProxy {
	if size > 0 {
		p.bufferSize = size
	}
	return p
}

// Start begins the periodic identification loop that flushes buffered messages
// once the singleton becomes reachable.
func (p *ClusterSingletonProxy) Start() {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return
	}
	p.started = true
	p.mu.Unlock()

	if p.identificationInterval <= 0 {
		return
	}
	go p.identificationLoop()
}

// Stop terminates the periodic identification loop.
func (p *ClusterSingletonProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		close(p.stopCh)
		p.started = false
	}
}

// identificationLoop periodically attempts to flush buffered messages.
func (p *ClusterSingletonProxy) identificationLoop() {
	ticker := time.NewTicker(p.identificationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.flushBuffer()
		}
	}
}

// flushBuffer attempts to deliver all buffered messages to the singleton.
func (p *ClusterSingletonProxy) flushBuffer() {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return
	}
	// Check if singleton is now reachable
	_, err := p.currentOldestPathLocked()
	if err != nil {
		p.mu.Unlock()
		return
	}
	buf := p.buffer
	p.buffer = nil
	p.mu.Unlock()

	for _, m := range buf {
		if m.senderPath != "" {
			path, _ := p.CurrentOldestPath()
			_ = p.router.SendWithSender(m.ctx, path, m.senderPath, m.msg)
		} else {
			_ = p.Send(m.ctx, m.msg)
		}
	}
}

// bufferMessage adds a message to the buffer, dropping the oldest if full.
func (p *ClusterSingletonProxy) bufferMessage(msg bufferedMessage) {
	if p.bufferSize <= 0 {
		return
	}
	if len(p.buffer) >= p.bufferSize {
		// Drop oldest — matches Pekko's ClusterSingletonProxy warning behavior
		log.Printf("WARNING: Singleton proxy buffer is full (%d). Dropping oldest message.", p.bufferSize)
		p.buffer = p.buffer[1:]
	}
	p.buffer = append(p.buffer, msg)
}

// currentOldestPathLocked returns the path without acquiring the mutex (caller holds it).
func (p *ClusterSingletonProxy) currentOldestPathLocked() (string, error) {
	var ua *gproto_cluster.UniqueAddress
	if p.dataCenter != "" {
		ua = p.cm.OldestNodeInDC(p.dataCenter, p.role)
	} else {
		ua = p.cm.OldestNode(p.role)
	}
	if ua == nil {
		return "", fmt.Errorf("ClusterSingletonProxy: no oldest node available (cluster state not yet known)")
	}
	addr := ua.GetAddress()
	path := p.managerPath
	if p.singletonName != "" {
		path = path + "/" + p.singletonName
	}
	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}
	return fmt.Sprintf("%s://%s@%s:%d%s",
		addr.GetProtocol(),
		addr.GetSystem(),
		addr.GetHostname(),
		addr.GetPort(),
		path), nil
}

// WithDataCenter restricts routing to the oldest node in the given data center.
func (p *ClusterSingletonProxy) WithDataCenter(dc string) cluster.ClusterSingletonProxyInterface {
	p.dataCenter = dc
	return p
}

// WithSingletonName sets the name of the singleton actor.
// In Pekko, the singleton is typically a child of the manager named "singleton".
// Set to empty string if the manager itself is the singleton.
func (p *ClusterSingletonProxy) WithSingletonName(name string) cluster.ClusterSingletonProxyInterface {
	p.singletonName = name
	return p
}

// ManagerPath returns the relative actor path of the singleton manager.
func (p *ClusterSingletonProxy) ManagerPath() string {
	return p.managerPath
}

// CurrentOldestPath returns the full Pekko actor path to the singleton on the current oldest node.
// Returns an error if no eligible node is known yet.
func (p *ClusterSingletonProxy) CurrentOldestPath() (string, error) {
	var ua *gproto_cluster.UniqueAddress
	if p.dataCenter != "" {
		ua = p.cm.OldestNodeInDC(p.dataCenter, p.role)
	} else {
		ua = p.cm.OldestNode(p.role)
	}
	if ua == nil {
		return "", fmt.Errorf("ClusterSingletonProxy: no oldest node available (cluster state not yet known)")
	}
	addr := ua.GetAddress()
	path := p.managerPath
	if p.singletonName != "" {
		path = path + "/" + p.singletonName
	}
	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}
	return fmt.Sprintf("%s://%s@%s:%d%s",
		addr.GetProtocol(),
		addr.GetSystem(),
		addr.GetHostname(),
		addr.GetPort(),
		path), nil
}

// Send resolves the oldest cluster node and delivers msg to the singleton actor there.
// If the oldest node changes between calls, subsequent sends automatically re-route.
// When the singleton location is unknown and buffering is enabled, the message is
// buffered and delivered once the proxy identifies the singleton.
func (p *ClusterSingletonProxy) Send(ctx context.Context, msg any) error {
	path, err := p.CurrentOldestPath()
	if err != nil {
		if p.bufferSize > 0 {
			p.mu.Lock()
			p.bufferMessage(bufferedMessage{ctx: ctx, msg: msg})
			p.mu.Unlock()
			return nil
		}
		return err
	}
	return p.router.Send(ctx, path, msg)
}

// SendWithSender delivers a message to the singleton actor with an explicit sender path.
// When the singleton location is unknown and buffering is enabled, the message is buffered.
func (p *ClusterSingletonProxy) SendWithSender(ctx context.Context, dest string, senderPath string, msg any) error {
	path, err := p.CurrentOldestPath()
	if err != nil {
		if p.bufferSize > 0 {
			p.mu.Lock()
			p.bufferMessage(bufferedMessage{ctx: ctx, msg: msg, senderPath: senderPath})
			p.mu.Unlock()
			return nil
		}
		return err
	}
	return p.router.SendWithSender(ctx, path, senderPath, msg)
}

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

	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
)

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
}

func NewClusterSingletonProxy(cm *cluster.ClusterManager, router cluster.Router, managerPath, role string) *ClusterSingletonProxy {
	return &ClusterSingletonProxy{
		cm:            cm,
		router:        router,
		managerPath:   managerPath,
		singletonName: "singleton",
		role:          role,
	}
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
func (p *ClusterSingletonProxy) Send(ctx context.Context, msg interface{}) error {
	path, err := p.CurrentOldestPath()
	if err != nil {
		return err
	}
	return p.router.Send(ctx, path, msg)
}

// SendWithSender delivers a message to the singleton actor with an explicit sender path.
func (p *ClusterSingletonProxy) SendWithSender(ctx context.Context, path string, senderPath string, msg any) error {
	dest, err := p.CurrentOldestPath()
	if err != nil {
		return err
	}
	return p.router.SendWithSender(ctx, dest, senderPath, msg)
}

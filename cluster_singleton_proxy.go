/*
 * cluster_singleton_proxy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
)

// ClusterSingletonProxy routes messages to the singleton actor on the oldest cluster node.
// In Pekko, ClusterSingletonManager names the child actor "singleton" by default.
// So a manager at "/user/singletonManager" hosts the singleton at "/user/singletonManager/singleton".
type ClusterSingletonProxy struct {
	cm          *ClusterManager
	router      *Router
	managerPath string // relative actor path of the singleton manager, e.g. "/user/singletonManager"
	role        string // optional role filter; empty means any node
}

func NewClusterSingletonProxy(cm *ClusterManager, router *Router, managerPath, role string) *ClusterSingletonProxy {
	return &ClusterSingletonProxy{
		cm:          cm,
		router:      router,
		managerPath: managerPath,
		role:        role,
	}
}

// CurrentOldestPath returns the full Pekko actor path to the singleton on the current oldest node.
// Returns an error if no eligible node is known yet.
func (p *ClusterSingletonProxy) CurrentOldestPath() (string, error) {
	ua := p.cm.OldestNode(p.role)
	if ua == nil {
		return "", fmt.Errorf("ClusterSingletonProxy: no oldest node available (cluster state not yet known)")
	}
	addr := ua.GetAddress()
	return fmt.Sprintf("%s://%s@%s:%d%s/singleton",
		addr.GetProtocol(),
		addr.GetSystem(),
		addr.GetHostname(),
		addr.GetPort(),
		p.managerPath), nil
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

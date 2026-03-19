/*
 * singleton.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"github.com/sopranoworks/gekka/actor"
)

// ClusterSingletonManagerInterface defines the minimal interface for a singleton manager.
type ClusterSingletonManagerInterface interface {
	actor.Actor
	WithDataCenter(dc string) ClusterSingletonManagerInterface
}

// ClusterSingletonProxyInterface defines the minimal interface for a singleton proxy.
type ClusterSingletonProxyInterface interface {
	Send(ctx context.Context, msg any) error
	SendWithSender(ctx context.Context, dest, senderPath string, msg any) error
	ManagerPath() string
	WithSingletonName(name string) ClusterSingletonProxyInterface
	WithDataCenter(dc string) ClusterSingletonProxyInterface
	CurrentOldestPath() (string, error)
}

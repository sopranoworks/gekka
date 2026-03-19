/*
 * typed_singleton.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"fmt"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster"
)

// Singleton manages the lifecycle of a typed cluster singleton.
type Singleton[M any] struct {
	manager *cluster.ClusterSingletonManager
}

// NewTypedSingleton creates a new type-safe singleton manager factory.
func NewTypedSingleton[M any](cm *cluster.ClusterManager, behavior typed.Behavior[M], role string) *Singleton[M] {
	props := actor.Props{
		New: func() actor.Actor {
			return typed.NewTypedActor(behavior)
		},
	}
	return &Singleton[M]{
		manager: cluster.NewClusterSingletonManager(cm, props, role),
	}
}

// WithDataCenter restricts the singleton to the oldest node in the given DC.
func (s *Singleton[M]) WithDataCenter(dc string) *Singleton[M] {
	s.manager.WithDataCenter(dc)
	return s
}

// Props returns the Props for spawning the singleton manager actor.
func (s *Singleton[M]) Props() actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return s.manager
		},
	}
}

// ─── Typed Singleton Proxy ───────────────────────────────────────────────

// TypedSingletonProxy provides type-safe access to a cluster singleton.
type TypedSingletonProxy[M any] struct {
	proxy *cluster.ClusterSingletonProxy
}

// NewTypedSingletonProxy creates a new type-safe proxy for a cluster singleton.
func NewTypedSingletonProxy[M any](cm *cluster.ClusterManager, router cluster.Router, managerPath, role string) *TypedSingletonProxy[M] {
	return &TypedSingletonProxy[M]{
		proxy: cluster.NewClusterSingletonProxy(cm, router, managerPath, role),
	}
}

// Tell sends a message of type M to the singleton.
func (p *TypedSingletonProxy[M]) Tell(msg M) {
	// Send fire-and-forget message via background context.
	_ = p.proxy.Send(context.Background(), msg)
}

// Path returns the path of the proxy (not the singleton itself).
func (p *TypedSingletonProxy[M]) Path() string {
	return p.proxy.ManagerPath() // Need to check if managerPath is exported or has getter
}

// Untyped returns the underlying untyped proxy.
func (p *TypedSingletonProxy[M]) Untyped() *cluster.ClusterSingletonProxy {
	return p.proxy
}

// Ask sends a message to the singleton and waits for a reply.
func (p *TypedSingletonProxy[M]) Ask(ctx context.Context, timeout time.Duration, msg any) (any, error) {
	// This would integrate with the system-wide Ask logic once fully implemented for proxies.
	return nil, fmt.Errorf("TypedSingletonProxy.Ask: not fully implemented")
}

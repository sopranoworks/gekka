/*
 * gekka_node_watch.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// ── Remote Death Watch ────────────────────────────────────────────────────────

func (c *Cluster) watchRemote(watcher ActorRef, target ActorRef) {
	ap, err := actor.ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Address.Host, ap.Address.Port)

	c.remoteWatchersMu.Lock()
	defer c.remoteWatchersMu.Unlock()

	targets, ok := c.remoteWatchers[nodeAddr]
	if !ok {
		targets = make(map[string][]ActorRef)
		c.remoteWatchers[nodeAddr] = targets
	}
	targets[target.Path()] = append(targets[target.Path()], watcher)
}

func (c *Cluster) unwatchRemote(watcher ActorRef, target ActorRef) {
	ap, err := actor.ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Address.Host, ap.Address.Port)

	c.remoteWatchersMu.Lock()
	defer c.remoteWatchersMu.Unlock()

	targets, ok := c.remoteWatchers[nodeAddr]
	if !ok {
		return
	}
	watchers := targets[target.Path()]
	for i, w := range watchers {
		if w == watcher {
			targets[target.Path()] = append(watchers[:i], watchers[i+1:]...)
			break
		}
	}
}

func (c *Cluster) triggerRemoteNodeDeath(addr cluster.MemberAddress) {
	nodeAddr := fmt.Sprintf("%s:%d", addr.Host, addr.Port)

	c.remoteWatchersMu.Lock()
	targets, ok := c.remoteWatchers[nodeAddr]
	if ok {
		delete(c.remoteWatchers, nodeAddr)
	}
	c.remoteWatchersMu.Unlock()

	if !ok {
		return
	}

	for targetPath, watchers := range targets {
		targetRef := ActorRef{fullPath: targetPath, node: c}
		terminatedMsg := Terminated{Actor: targetRef}
		for _, w := range watchers {
			w.Tell(terminatedMsg)
		}
	}
}

type remoteDeathWatcherActor struct {
	actor.BaseActor
	cluster *Cluster
}

func (a *remoteDeathWatcherActor) Receive(msg any) {
	switch e := msg.(type) {
	case cluster.UnreachableMember:
		a.cluster.triggerRemoteNodeDeath(e.Member)
	case cluster.MemberRemoved:
		a.cluster.triggerRemoteNodeDeath(e.Member)
	}
}

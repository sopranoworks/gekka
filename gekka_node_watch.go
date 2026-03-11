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

func (n *GekkaNode) watchRemote(watcher ActorRef, target ActorRef) {
	ap, err := ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Host, ap.Port)

	n.remoteWatchersMu.Lock()
	defer n.remoteWatchersMu.Unlock()

	targets, ok := n.remoteWatchers[nodeAddr]
	if !ok {
		targets = make(map[string][]ActorRef)
		n.remoteWatchers[nodeAddr] = targets
	}
	targets[target.Path()] = append(targets[target.Path()], watcher)
}

func (n *GekkaNode) unwatchRemote(watcher ActorRef, target ActorRef) {
	ap, err := ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Host, ap.Port)

	n.remoteWatchersMu.Lock()
	defer n.remoteWatchersMu.Unlock()

	targets, ok := n.remoteWatchers[nodeAddr]
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

func (n *GekkaNode) triggerRemoteNodeDeath(addr cluster.MemberAddress) {
	nodeAddr := fmt.Sprintf("%s:%d", addr.Host, addr.Port)

	n.remoteWatchersMu.Lock()
	targets, ok := n.remoteWatchers[nodeAddr]
	if ok {
		delete(n.remoteWatchers, nodeAddr)
	}
	n.remoteWatchersMu.Unlock()

	if !ok {
		return
	}

	for targetPath, watchers := range targets {
		targetRef := ActorRef{fullPath: targetPath, node: n}
		terminatedMsg := Terminated{Actor: targetRef}
		for _, w := range watchers {
			w.Tell(terminatedMsg)
		}
	}
}

type remoteDeathWatcherActor struct {
	actor.BaseActor
	node *GekkaNode
}

func (a *remoteDeathWatcherActor) Receive(msg any) {
	switch e := msg.(type) {
	case cluster.UnreachableMember:
		a.node.triggerRemoteNodeDeath(e.Member)
	case cluster.MemberRemoved:
		a.node.triggerRemoteNodeDeath(e.Member)
	}
}

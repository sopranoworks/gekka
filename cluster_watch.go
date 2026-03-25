/*
 * cluster_watch.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"log/slog"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// ── Remote Death Watch ────────────────────────────────────────────────────────

func (c *Cluster) watchRemote(watcher ActorRef, target ActorRef) {
	ap, err := actor.ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Address.Host, ap.Address.Port)

	c.remoteWatchersMu.Lock()
	targets, ok := c.remoteWatchers[nodeAddr]
	if !ok {
		targets = make(map[string][]ActorRef)
		c.remoteWatchers[nodeAddr] = targets
	}
	targets[target.Path()] = append(targets[target.Path()], watcher)
	c.remoteWatchersMu.Unlock()

	// Send WATCH system message to remote node
	assoc, ok := c.nm.GetGekkaAssociationByHost(ap.Address.Host, uint32(ap.Address.Port))
	if !ok {
		slog.Debug("artery: cannot send WATCH, no association for remote", "host", ap.Address.Host, "port", ap.Address.Port)
		return
	}

	sm := &gproto_remote.SystemMessage{
		Type: gproto_remote.SystemMessage_WATCH.Enum(),
		WatchData: &gproto_remote.WatchData{
			Watchee: &gproto_remote.ProtoActorRef{
				Path: proto.String(target.Path()),
			},
			Watcher: &gproto_remote.ProtoActorRef{
				Path: proto.String(watcher.Path()),
			},
		},
	}
	smBytes, err := proto.Marshal(sm)
	if err != nil {
		return
	}

	seq := c.lastSystemSeqNo.Add(1)
	env := &gproto_remote.SystemMessageEnvelope{
		Message:         smBytes,
		SerializerId:    proto.Int32(2), // Protobuf
		MessageManifest: []byte("SystemMessage"),
		SeqNo:           proto.Uint64(seq),
		AckReplyTo: &gproto_remote.UniqueAddress{
			Address: c.nm.LocalAddr,
			Uid:     proto.Uint64(assoc.LocalUid()),
			},
			}
			payload, err := proto.Marshal(env)
			if err != nil {
			return
			}

			frame, err := core.BuildArteryFrame(
			int64(assoc.LocalUid()),
			17, // ArteryInternalSerializerID
			"",
			target.Path(),
			"SystemMessage",
			payload,
			true,
			)
			if err != nil {
			return
			}

			select {
			case assoc.Outbox() <- frame:

	default:
		slog.Debug("artery: outbox full, dropping WATCH system message")
	}
}

func (c *Cluster) unwatchRemote(watcher ActorRef, target ActorRef) {
	ap, err := actor.ParseActorPath(target.Path())
	if err != nil {
		return
	}
	nodeAddr := fmt.Sprintf("%s:%d", ap.Address.Host, ap.Address.Port)

	c.remoteWatchersMu.Lock()
	targets, ok := c.remoteWatchers[nodeAddr]
	if !ok {
		c.remoteWatchersMu.Unlock()
		return
	}
	watchers := targets[target.Path()]
	for i, w := range watchers {
		if w == watcher {
			targets[target.Path()] = append(watchers[:i], watchers[i+1:]...)
			break
		}
	}
	c.remoteWatchersMu.Unlock()

	// Send UNWATCH system message to remote node
	assoc, ok := c.nm.GetGekkaAssociationByHost(ap.Address.Host, uint32(ap.Address.Port))
	if !ok {
		return
	}

	sm := &gproto_remote.SystemMessage{
		Type: gproto_remote.SystemMessage_UNWATCH.Enum(),
		WatchData: &gproto_remote.WatchData{
			Watchee: &gproto_remote.ProtoActorRef{
				Path: proto.String(target.Path()),
			},
			Watcher: &gproto_remote.ProtoActorRef{
				Path: proto.String(watcher.Path()),
			},
		},
	}
	smBytes, err := proto.Marshal(sm)
	if err != nil {
		return
	}

	seq := c.lastSystemSeqNo.Add(1)
	env := &gproto_remote.SystemMessageEnvelope{
		Message:         smBytes,
		SerializerId:    proto.Int32(2), // Protobuf
		MessageManifest: []byte("SystemMessage"),
		SeqNo:           proto.Uint64(seq),
		AckReplyTo: &gproto_remote.UniqueAddress{
			Address: c.nm.LocalAddr,
			Uid:     proto.Uint64(assoc.LocalUid()),
			},
			}
			payload, err := proto.Marshal(env)
			if err != nil {
			return
			}

			frame, err := core.BuildArteryFrame(
			int64(assoc.LocalUid()),
			17, // ArteryInternalSerializerID
			"",
			target.Path(),
			"SystemMessage",
			payload,
			true,
			)
			if err != nil {
			return
			}

			select {
			case assoc.Outbox() <- frame:

	default:
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
		targetRef := ActorRef{fullPath: targetPath, sys: c}
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

// HandleSystemMessage processes inbound system messages from the Artery layer.
func (c *Cluster) HandleSystemMessage(remote *gproto_remote.UniqueAddress, env *gproto_remote.SystemMessageEnvelope, sm *gproto_remote.SystemMessage) error {
	remoteID := core.UniqueAddressToString(remote)
	switch sm.GetType() {
	case gproto_remote.SystemMessage_WATCH:
		data := sm.GetWatchData()
		if data == nil {
			return fmt.Errorf("WATCH message missing WatchData")
		}
		targetPath := data.GetWatchee().GetPath()
		watcher := data.GetWatcher()

		c.localWatchersMu.Lock()
		nodeWatchers, ok := c.localWatchers[targetPath]
		if !ok {
			nodeWatchers = make(map[string][]*gproto_remote.ProtoActorRef)
			c.localWatchers[targetPath] = nodeWatchers
		}
		nodeWatchers[remoteID] = append(nodeWatchers[remoteID], watcher)
		c.localWatchersMu.Unlock()
		slog.Debug("artery: registered remote watcher", "watchee", targetPath, "watcher", watcher.GetPath(), "remote", remoteID)

	case gproto_remote.SystemMessage_UNWATCH:
		data := sm.GetWatchData()
		if data == nil {
			return fmt.Errorf("UNWATCH message missing WatchData")
		}
		targetPath := data.GetWatchee().GetPath()
		watcherPath := data.GetWatcher().GetPath()

		c.localWatchersMu.Lock()
		if nodeWatchers, ok := c.localWatchers[targetPath]; ok {
			if watchers, ok := nodeWatchers[remoteID]; ok {
				for i, w := range watchers {
					if w.GetPath() == watcherPath {
						nodeWatchers[remoteID] = append(watchers[:i], watchers[i+1:]...)
						break
					}
				}
			}
		}
		c.localWatchersMu.Unlock()
		slog.Debug("artery: removed remote watcher", "watchee", targetPath, "watcher", watcherPath, "remote", remoteID)
	}
	return nil
}

// triggerLocalActorDeath notifies all remote watchers that a local actor has terminated.
func (c *Cluster) triggerLocalActorDeath(path string, ref ActorRef) {
	c.localWatchersMu.Lock()
	nodeWatchers, ok := c.localWatchers[path]
	if ok {
		delete(c.localWatchers, path)
	}
	c.localWatchersMu.Unlock()

	if !ok {
		return
	}

	for remoteID, watchers := range nodeWatchers {
		assoc, ok := c.nm.GetAssociationByRemote(remoteID, 1) // Stream 1 for control
		if !ok {
			slog.Debug("artery: cannot send Terminated, no association for remote", "remote", remoteID)
			continue
		}

		for _, watcher := range watchers {
			// Construct Terminated system message
			sm := &gproto_remote.SystemMessage{
				Type: gproto_remote.SystemMessage_DEATHWATCH_NOTIFICATION.Enum(),
				DwNotificationData: &gproto_remote.DeathWatchNotificationData{
					Actor: &gproto_remote.ProtoActorRef{
						Path: proto.String(ref.Path()),
					},
				},
			}
			smBytes, err := proto.Marshal(sm)
			if err != nil {
				continue
			}

			seq := c.lastSystemSeqNo.Add(1)
			env := &gproto_remote.SystemMessageEnvelope{
				Message:         smBytes,
				SerializerId:    proto.Int32(2), // Protobuf
				MessageManifest: []byte("SystemMessage"),
				SeqNo:           proto.Uint64(seq),
				AckReplyTo: &gproto_remote.UniqueAddress{
					Address: c.nm.LocalAddr,
					Uid:     proto.Uint64(assoc.LocalUid()),
				},
			}

			payload, err := proto.Marshal(env)
			if err != nil {
				continue
			}

			// Wrap in Artery frame with manifest "SystemMessage"
			frame, err := core.BuildArteryFrame(
				int64(assoc.LocalUid()),
				17, // ArteryInternalSerializerID
				"",
				watcher.GetPath(),
				"SystemMessage",
				payload,
				true,
			)
			if err != nil {
				continue
			}

			select {
			case assoc.Outbox() <- frame:
			default:
				slog.Debug("artery: outbox full, dropping Terminated notification")
			}
		}
	}
}

/*
 * cluster_watch.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/internal/core"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// watchFDState carries the runtime bookkeeping needed by the remote-watch
// failure-detector reaper. It is a separate struct so callers that never
// register a remote watch pay no allocation / goroutine cost.
type watchFDState struct {
	once     sync.Once
	cancel   context.CancelFunc
	mu       sync.Mutex
	keyToAddr map[string]cluster.MemberAddress
}

func (c *Cluster) watchFD() *watchFDState {
	c.watchFDOnceInit.Do(func() {
		c.watchFDStateRef = &watchFDState{
			keyToAddr: make(map[string]cluster.MemberAddress),
		}
	})
	return c.watchFDStateRef
}

// registerWatchedNode records (key → MemberAddress) so the reaper can map a
// detector key back to the address required by triggerRemoteNodeDeath. Also
// seeds an initial heartbeat so IsAvailable starts in the "available" state.
func (c *Cluster) registerWatchedNode(key string, addr cluster.MemberAddress) {
	if c.cm == nil || c.cm.WatchFd == nil {
		return
	}
	state := c.watchFD()
	state.mu.Lock()
	state.keyToAddr[key] = addr
	state.mu.Unlock()
	// Seed a heartbeat so IsAvailable starts available; subsequent real
	// heartbeats from the cluster heartbeat path keep it healthy.
	c.cm.WatchFd.Heartbeat(key)
}

// startWatchFDReaper lazily starts a single reaper goroutine that polls the
// watch failure detector at UnreachableNodesReaperInterval and triggers a
// remote-death notification when IsAvailable flips for a watched node.
func (c *Cluster) startWatchFDReaper() {
	if c.cm == nil || c.cm.WatchFd == nil {
		return
	}
	state := c.watchFD()
	state.once.Do(func() {
		ctx, cancel := context.WithCancel(c.ctx)
		state.cancel = cancel
		go c.watchFDReaperLoop(ctx)
	})
}

func (c *Cluster) watchFDReaperLoop(ctx context.Context) {
	interval := c.cm.WatchFd.ReaperInterval()
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reapUnreachableWatched()
		}
	}
}

func (c *Cluster) reapUnreachableWatched() {
	if c.cm == nil || c.cm.WatchFd == nil {
		return
	}
	keys := c.cm.WatchFd.WatchedNodes()
	if len(keys) == 0 {
		return
	}
	state := c.watchFD()
	for _, key := range keys {
		if c.cm.WatchFd.IsAvailable(key) {
			continue
		}
		state.mu.Lock()
		addr, ok := state.keyToAddr[key]
		if ok {
			delete(state.keyToAddr, key)
		}
		state.mu.Unlock()
		// Drop the FD state regardless: a re-registered watch will reseed.
		c.cm.WatchFd.Remove(key)
		if !ok {
			continue
		}
		slog.Debug("watch-fd: remote node unreachable, triggering Terminated", "key", key, "host", addr.Host, "port", addr.Port)
		c.triggerRemoteNodeDeath(addr)
	}
}

// stopWatchFDReaper cancels the reaper goroutine if it was started. Called
// from Cluster.Shutdown.
func (c *Cluster) stopWatchFDReaper() {
	if c.watchFDStateRef == nil {
		return
	}
	if c.watchFDStateRef.cancel != nil {
		c.watchFDStateRef.cancel()
	}
}

// watchFDKeyForAssociation derives the host:port-uid key the cluster heartbeat
// path uses to feed the failure detector, given a remote association.
func watchFDKeyForAssociation(assoc *core.GekkaAssociation) (string, bool) {
	remote := assoc.Remote()
	if remote == nil {
		return "", false
	}
	addr := remote.GetAddress()
	if addr == nil {
		return "", false
	}
	// remote.UniqueAddress.Uid is the full uint64; the cluster heartbeat
	// path encodes the same uid by combining cluster.UniqueAddress.{Uid, Uid2}
	// into a uint64. Both representations yield the same key digit sequence.
	return fmt.Sprintf("%s:%d-%d", addr.GetHostname(), addr.GetPort(), remote.GetUid()), true
}


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

	// Register the remote node with the watch failure detector and lazy-start
	// the reaper goroutine. The detector key matches the cluster heartbeat
	// path's key format (host:port-uid) so cluster heartbeats already feed it.
	if key, kok := watchFDKeyForAssociation(assoc); kok {
		c.registerWatchedNode(key, cluster.MemberAddress{
			Host: ap.Address.Host,
			Port: uint32(ap.Address.Port),
		})
		c.startWatchFDReaper()
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

	// Drop any watch-FD bookkeeping for this address so the reaper does not
	// re-fire for the same node. The mapping is keyed by host:port-uid; we
	// purge every key whose host:port matches.
	if c.watchFDStateRef != nil && c.cm != nil && c.cm.WatchFd != nil {
		state := c.watchFDStateRef
		state.mu.Lock()
		var matched []string
		for k, ka := range state.keyToAddr {
			if ka.Host == addr.Host && ka.Port == addr.Port {
				matched = append(matched, k)
			}
		}
		for _, k := range matched {
			delete(state.keyToAddr, k)
		}
		state.mu.Unlock()
		for _, k := range matched {
			c.cm.WatchFd.Remove(k)
		}
	}

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
//
// Sub-plan 8i: cross-network DeathWatchNotification emission is delayed by
// EffectiveDeathWatchNotificationFlushTimeout so prior messages from the
// dying actor have a chance to flush to the watcher first. The local
// Terminated dispatch path (driven by SetOnStop in cluster.go) is unaffected
// — only the remote notification is deferred. If the node is shutting down
// (c.ctx cancelled) the deferred goroutine returns without emitting; the
// before-actor-system-terminate flush task already covers any in-flight
// frames in that case.
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

	go func() {
		if !core.DeathWatchNotificationFlushDelay(c.ctx, c.nm.EffectiveDeathWatchNotificationFlushTimeout()) {
			return
		}
		c.emitRemoteDeathWatchNotifications(nodeWatchers, ref)
	}()
}

// emitRemoteDeathWatchNotifications sends one DeathWatchNotification frame
// per remote watcher. Extracted from triggerLocalActorDeath in sub-plan 8i so
// the emission can be invoked after a flush delay.
func (c *Cluster) emitRemoteDeathWatchNotifications(nodeWatchers map[string][]*gproto_remote.ProtoActorRef, ref ActorRef) {
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

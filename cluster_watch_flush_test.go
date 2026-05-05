/*
 * cluster_watch_flush_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestTriggerLocalActorDeath_DoesNotBlockOnFlushTimeout verifies that
// triggerLocalActorDeath returns immediately and defers the cross-network
// DeathWatchNotification emission to a goroutine, so the actor stop callback
// is never blocked by EffectiveDeathWatchNotificationFlushTimeout.
//
// Sub-plan 8i: this is the production observation that the
// pekko.remote.artery.advanced.death-watch-notification-flush-timeout config
// path is consumed at runtime (rather than just plumbed onto NodeManager).
func TestTriggerLocalActorDeath_DoesNotBlockOnFlushTimeout(t *testing.T) {
	c := newTestNode(t, "FlushSys", "127.0.0.1", 0)
	defer func() { _ = c.Shutdown() }()

	c.localWatchers = map[string]map[string][]*gproto_remote.ProtoActorRef{}
	c.nm.DeathWatchNotificationFlushTimeout = 5 * time.Second

	// Seed a fake remote watcher entry. No association exists for the
	// remote, so the deferred goroutine will eventually log "no
	// association for remote" and exit — the test does not assert on
	// frame delivery, only on caller non-blocking and side effects.
	const targetPath = "pekko://FlushSys@127.0.0.1:0/user/target"
	c.localWatchers[targetPath] = map[string][]*gproto_remote.ProtoActorRef{
		"10.0.0.1:2551:777": {
			{Path: proto.String("pekko://Remote@10.0.0.1:2551/user/watcher")},
		},
	}

	ref := ActorRef{fullPath: targetPath, sys: c}

	start := time.Now()
	c.triggerLocalActorDeath(targetPath, ref)
	elapsed := time.Since(start)

	if elapsed >= 200*time.Millisecond {
		t.Errorf("triggerLocalActorDeath blocked %v; want < 200ms (flush deferred to goroutine, configured timeout=5s)", elapsed)
	}

	// triggerLocalActorDeath also deletes the watcher entry synchronously
	// before spawning the goroutine — verify the cleanup happened.
	c.localWatchersMu.Lock()
	_, stillPresent := c.localWatchers[targetPath]
	c.localWatchersMu.Unlock()
	if stillPresent {
		t.Errorf("localWatchers[%q] should be removed synchronously", targetPath)
	}
}

// TestTriggerLocalActorDeath_ZeroFlushTimeoutEmitsImmediately verifies that a
// zero-valued DeathWatchNotificationFlushTimeout falls back to the Pekko
// default rather than skipping the delay. This is a guard against
// accidentally collapsing the flush window when a user sets the field to
// zero (the Effective getter substitutes the default).
func TestTriggerLocalActorDeath_ZeroFlushTimeoutEmitsImmediately(t *testing.T) {
	c := newTestNode(t, "FlushSys", "127.0.0.1", 0)
	defer func() { _ = c.Shutdown() }()

	c.localWatchers = map[string]map[string][]*gproto_remote.ProtoActorRef{}

	// 1ms is the smallest non-zero configurable value; effective getter
	// returns it directly. A zero would have triggered the default
	// fallback (3s), which would make this test slow.
	c.nm.DeathWatchNotificationFlushTimeout = 1 * time.Millisecond

	const targetPath = "pekko://FlushSys@127.0.0.1:0/user/target"
	c.localWatchers[targetPath] = map[string][]*gproto_remote.ProtoActorRef{
		"10.0.0.1:2551:777": {
			{Path: proto.String("pekko://Remote@10.0.0.1:2551/user/watcher")},
		},
	}

	ref := ActorRef{fullPath: targetPath, sys: c}

	start := time.Now()
	c.triggerLocalActorDeath(targetPath, ref)
	elapsed := time.Since(start)

	if elapsed >= 100*time.Millisecond {
		t.Errorf("triggerLocalActorDeath blocked %v; want < 100ms (returns before goroutine sleep)", elapsed)
	}
}

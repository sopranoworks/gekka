/*
 * cluster_watch_fd_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

// Verifies the watch-FD reaper consults the configured
// acceptable-heartbeat-pause and unreachable-nodes-reaper-interval at runtime
// and drives Cluster.triggerRemoteNodeDeath when the detector flips a watched
// node to unavailable.
//
// Builds a Cluster with a stubbed ClusterManager carrying a configured
// WatchFailureDetector, registers a watcher actor against a fake remote node
// path, lazy-starts the reaper through the same code path watchRemote uses,
// and asserts the watcher receives Terminated within the pause+reaper window
// and NOT before the pause has elapsed.
func TestWatchFD_ReaperFires_AfterAcceptablePause(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	defer func() { _ = node.Shutdown() }()

	// Wire a ClusterManager stub with a watch FD configured to aggressive
	// thresholds. We only need the WatchFd field for the reaper code path.
	node.cm = &cluster.ClusterManager{
		WatchFd: cluster.NewWatchFailureDetector(cluster.WatchFailureDetectorConfig{
			Threshold:                      8.0,
			MaxSampleSize:                  200,
			MinStdDeviation:                50 * time.Millisecond,
			AcceptableHeartbeatPause:       150 * time.Millisecond,
			UnreachableNodesReaperInterval: 50 * time.Millisecond,
		}),
	}

	// Watcher actor that records Terminated arrivals.
	watcherActor := &watcherTestingActor{
		BaseActor:  actor.NewBaseActor(),
		terminated: make(chan Terminated, 5),
	}
	watcherRef, err := node.System.ActorOf(Props{
		New: func() actor.Actor { return watcherActor },
	}, "watcher-fd")
	if err != nil {
		t.Fatalf("ActorOf watcher: %v", err)
	}

	// Register a fake remote target on a synthetic remote node.
	const fakeHost = "192.0.2.10" // RFC5737 TEST-NET-1
	const fakePort uint32 = 26001
	const fakeUid uint64 = 0xCAFEBABE
	targetPath := "pekko://Sys@192.0.2.10:26001/user/remote-target"
	nodeAddrStr := "192.0.2.10:26001"

	node.remoteWatchersMu.Lock()
	node.remoteWatchers[nodeAddrStr] = map[string][]ActorRef{
		targetPath: {watcherRef},
	}
	node.remoteWatchersMu.Unlock()

	// Register the node into the watch FD using the same key format the
	// cluster heartbeat path uses (host:port-uid). Lazy-start the reaper
	// through the production helper.
	key := "192.0.2.10:26001-3405691582" // matches fakeUid in decimal
	_ = fakeHost
	_ = fakePort
	_ = fakeUid
	node.registerWatchedNode(key, cluster.MemberAddress{
		Host: fakeHost,
		Port: fakePort,
	})
	node.startWatchFDReaper()

	// Before the acceptable-heartbeat-pause elapses, no Terminated must arrive.
	select {
	case term := <-watcherActor.terminated:
		t.Fatalf("Terminated %v fired before pause window", term)
	case <-time.After(60 * time.Millisecond):
		// expected: still inside the 150ms acceptable-pause window
	}

	// After the pause + a couple of reaper ticks, Terminated must arrive.
	select {
	case term := <-watcherActor.terminated:
		if term.Actor.Path() != targetPath {
			t.Errorf("Terminated path = %q, want %q", term.Actor.Path(), targetPath)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Terminated from watch-FD reaper")
	}
}

// Verifies the same scenario with a long acceptable-heartbeat-pause (Pekko
// default 10s) does NOT fire Terminated within the test window — proving the
// pause value drives the runtime decision, not just the reaper interval.
func TestWatchFD_LongPause_DoesNotFire(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	defer func() { _ = node.Shutdown() }()

	node.cm = &cluster.ClusterManager{
		WatchFd: cluster.NewWatchFailureDetector(cluster.WatchFailureDetectorConfig{
			Threshold:                      8.0,
			MaxSampleSize:                  200,
			MinStdDeviation:                50 * time.Millisecond,
			AcceptableHeartbeatPause:       10 * time.Second, // Pekko default
			UnreachableNodesReaperInterval: 50 * time.Millisecond,
		}),
	}

	watcherActor := &watcherTestingActor{
		BaseActor:  actor.NewBaseActor(),
		terminated: make(chan Terminated, 5),
	}
	watcherRef, err := node.System.ActorOf(Props{
		New: func() actor.Actor { return watcherActor },
	}, "watcher-fd-long")
	if err != nil {
		t.Fatalf("ActorOf watcher: %v", err)
	}

	const targetPath = "pekko://Sys@192.0.2.11:26002/user/remote-target"
	const nodeAddrStr = "192.0.2.11:26002"
	node.remoteWatchersMu.Lock()
	node.remoteWatchers[nodeAddrStr] = map[string][]ActorRef{
		targetPath: {watcherRef},
	}
	node.remoteWatchersMu.Unlock()

	node.registerWatchedNode("192.0.2.11:26002-1", cluster.MemberAddress{
		Host: "192.0.2.11",
		Port: 26002,
	})
	node.startWatchFDReaper()

	select {
	case term := <-watcherActor.terminated:
		t.Fatalf("Terminated %v fired despite 10s acceptable-pause; pause value not consulted", term)
	case <-time.After(500 * time.Millisecond):
		// expected: 10s pause keeps the node available
	}
}

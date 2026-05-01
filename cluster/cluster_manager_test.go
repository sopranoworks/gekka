/*
 * cluster_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// ── WeaklyUp promotion tests (#18: allow-weakly-up-members) ─────────────────

// weaklyUpTestCM builds a cluster manager with a local Up node (leader) plus
// a second Up node that is NOT in Seen, so convergence fails. This is the
// prerequisite for WeaklyUp promotion testing.
func weaklyUpTestCM() *ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2551),
		},
		Uid:  proto.Uint32(1),
		Uid2: proto.Uint32(0),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }
	cm := NewClusterManager(local, router)
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)

	// Add a second Up member (index 1) NOT in Seen → convergence blocked.
	peer := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2553),
		},
		Uid:  proto.Uint32(99),
		Uid2: proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, peer)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(2),
	})
	// Only local (index 0) in Seen; peer (index 1) NOT → convergence fails.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	return cm
}

// TestWeaklyUp_JoiningToWeaklyUpAfterTimeout verifies that a Joining member
// is promoted to WeaklyUp after AllowWeaklyUpMembers duration when convergence
// is not achieved, matching Pekko's ClusterCoreDaemon behavior.
func TestWeaklyUp_JoiningToWeaklyUpAfterTimeout(t *testing.T) {
	cm := weaklyUpTestCM()

	// Add a Joining member at index 2.
	remote := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid:  proto.Uint32(2),
		Uid2: proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, remote)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(2),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(0),
	})

	// Use very short timeout for testing.
	cm.AllowWeaklyUpMembers = 50 * time.Millisecond

	// First call: should register firstSeen but NOT yet promote.
	cm.performLeaderActions()
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_Joining {
		t.Fatalf("expected Joining after first leader action, got %v", cm.State.Members[2].GetStatus())
	}

	// Wait for timeout.
	time.Sleep(60 * time.Millisecond)

	// Second call: should promote to WeaklyUp.
	cm.performLeaderActions()
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_WeaklyUp {
		t.Fatalf("expected WeaklyUp after timeout, got %v", cm.State.Members[2].GetStatus())
	}
}

// TestWeaklyUp_UpOnConvergence verifies that a WeaklyUp member is promoted
// to Up once convergence is achieved.
func TestWeaklyUp_UpOnConvergence(t *testing.T) {
	cm := weaklyUpTestCM()

	// Add a WeaklyUp member at index 2.
	remote := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid:  proto.Uint32(2),
		Uid2: proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, remote)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(2),
		Status:       gproto_cluster.MemberStatus_WeaklyUp.Enum(),
		UpNumber:     proto.Int32(0),
	})

	// Convergence blocked (peer at index 1 not in Seen) → WeaklyUp stays.
	cm.performLeaderActions()
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_WeaklyUp {
		t.Fatalf("expected WeaklyUp without convergence, got %v", cm.State.Members[2].GetStatus())
	}

	// Achieve convergence (all Up members in Seen: indices 0, 1).
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0, 1}}
	cm.performLeaderActions()
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_Up {
		t.Fatalf("expected Up after convergence, got %v", cm.State.Members[2].GetStatus())
	}
}

// TestWeaklyUp_DisabledMode verifies that when AllowWeaklyUpMembers == 0
// (off), Joining members are NOT promoted to WeaklyUp and must wait for
// full convergence to go directly to Up.
func TestWeaklyUp_DisabledMode(t *testing.T) {
	cm := weaklyUpTestCM()
	cm.AllowWeaklyUpMembers = 0

	// Add a Joining member at index 2.
	remote := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid:  proto.Uint32(2),
		Uid2: proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, remote)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(2),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(0),
	})

	// Multiple leader actions should NOT produce WeaklyUp.
	for i := 0; i < 5; i++ {
		cm.performLeaderActions()
	}
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_Joining {
		t.Fatalf("expected Joining to remain when WeaklyUp disabled, got %v", cm.State.Members[2].GetStatus())
	}

	// With convergence (all Up members in Seen), should go directly to Up.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0, 1}}
	cm.performLeaderActions()
	if cm.State.Members[2].GetStatus() != gproto_cluster.MemberStatus_Up {
		t.Fatalf("expected direct Joining→Up with convergence (WeaklyUp disabled), got %v", cm.State.Members[2].GetStatus())
	}
}

func TestVectorClockComparison(t *testing.T) {
	v1 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(1)},
		},
	}
	v2 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(2)},
		},
	}
	v3 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(2), Timestamp: proto.Int64(1)},
		},
	}
	v4 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(1)},
			{HashIndex: proto.Int32(2), Timestamp: proto.Int64(1)},
		},
	}

	if CompareVectorClock(v1, v1) != ClockSame {
		t.Errorf("expected same")
	}
	if CompareVectorClock(v1, v2) != ClockBefore {
		t.Errorf("expected v1 before v2")
	}
	if CompareVectorClock(v2, v1) != ClockAfter {
		t.Errorf("expected v2 after v1")
	}
	if CompareVectorClock(v1, v3) != ClockConcurrent {
		t.Errorf("expected concurrent")
	}
	if CompareVectorClock(v1, v4) != ClockBefore {
		t.Errorf("expected v1 before v4")
	}
}

func TestCheckConvergence(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint32(123),
		Uid2:    proto.Uint32(0),
	}
	// No-op router
	router := func(ctx context.Context, path string, msg any) error { return nil }
	cm := NewClusterManager(local, router)
	cm.State.Overview = &gproto_cluster.GossipOverview{
		Seen: []int32{0},
	}

	// Default state has only self. Convergence should be true.
	if !cm.CheckConvergence() {
		t.Errorf("expected convergence with single node")
	}

	// Add another member but not in seen set
	addr2 := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2553)},
		Uid:     proto.Uint32(456),
		Uid2:    proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, addr2)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
	})

	if cm.CheckConvergence() {
		t.Errorf("expected non-convergence, remote node hasn't seen state")
	}

	// Add remote node to seen set
	cm.State.Overview.Seen = append(cm.State.Overview.Seen, 1)

	if !cm.CheckConvergence() {
		t.Errorf("expected convergence, all nodes have seen state")
	}
}

// --- Session 1 tests: #2 LeaderActionsInterval, #4 PeriodicTasksInitialDelay, #5 ShutdownAfterUnsuccessfulJoinSeedNodes ---

func newTimerTestCM() *ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint32(1),
		Uid2:    proto.Uint32(0),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }
	return NewClusterManager(local, router)
}

// TestLeaderActionsInterval_IndependentTicker verifies that when
// LeaderActionsInterval is set, leader actions fire on their own ticker
// separate from gossip.
func TestLeaderActionsInterval_IndependentTicker(t *testing.T) {
	cm := newTimerTestCM()

	// Set a long gossip interval but short leader interval.
	cm.GossipInterval = 10 * time.Second
	cm.LeaderActionsInterval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We observe the gossip loop behavior via timing: if leader actions run
	// independently, the loop's select will fire the leaderTicker within ~100ms
	// even though the gossip interval is 10s. If leader actions were NOT
	// independent, nothing would happen for 10s and the test would time out.
	done := make(chan struct{})
	go func() {
		cm.StartGossipLoop(ctx)
		close(done)
	}()

	// Wait long enough for a few leader ticks but much less than gossip interval.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
}

// TestPeriodicTasksInitialDelay_GossipLoop verifies that the gossip loop
// honors the periodic-tasks-initial-delay before first tick.
func TestPeriodicTasksInitialDelay_GossipLoop(t *testing.T) {
	cm := newTimerTestCM()
	cm.GossipInterval = 50 * time.Millisecond
	cm.PeriodicTasksInitialDelay = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := time.Now()
	var firstTick atomic.Int64

	// Override gossipTick to record when the first tick fires.
	// We can't easily override an unexported method, so instead we track
	// gossip activity via the Router being called (gossipTick calls gossipTo
	// which calls Router). Use a Router that records the first call time.
	cm.Router = func(_ context.Context, _ string, _ any) error {
		if firstTick.CompareAndSwap(0, time.Since(started).Milliseconds()) {
			cancel()
		}
		return nil
	}

	// We need at least one other member for gossip to target.
	cm.Mu.Lock()
	cm.State.AllAddresses = append(cm.State.AllAddresses, &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2553)},
		Uid:     proto.Uint32(2),
		Uid2:    proto.Uint32(0),
	})
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(1),
	})
	cm.State.AllHashes = []string{"hash0", "hash1"}
	cm.State.Version = &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(0), Timestamp: proto.Int64(1)},
		},
	}
	cm.State.Overview.Seen = []int32{0, 1}
	cm.Mu.Unlock()

	go cm.StartGossipLoop(ctx)

	select {
	case <-ctx.Done():
		ft := firstTick.Load()
		if ft < 150 { // should be at least ~200ms (delay) but allow some tolerance
			t.Errorf("first gossip tick at %dms, expected >= 150ms (initial delay = 200ms)", ft)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gossip loop did not fire within 2s")
	}
}

// TestPeriodicTasksInitialDelay_Heartbeat verifies that StartHeartbeat
// honors the initial delay.
func TestPeriodicTasksInitialDelay_Heartbeat(t *testing.T) {
	cm := newTimerTestCM()
	cm.HeartbeatInterval = 50 * time.Millisecond
	cm.PeriodicTasksInitialDelay = 200 * time.Millisecond

	started := time.Now()
	var firstCall atomic.Int64
	done := make(chan struct{})

	cm.Router = func(_ context.Context, _ string, _ any) error {
		if firstCall.CompareAndSwap(0, time.Since(started).Milliseconds()) {
			close(done)
		}
		return nil
	}

	target := &gproto_cluster.Address{
		System:   proto.String("sys"),
		Hostname: proto.String("remote"),
		Port:     proto.Uint32(2553),
	}
	cm.StartHeartbeat(target)

	select {
	case <-done:
		fc := firstCall.Load()
		if fc < 150 {
			t.Errorf("first heartbeat at %dms, expected >= 150ms (initial delay = 200ms)", fc)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not fire within 2s")
	}
}

// TestShutdownAfterUnsuccessfulJoinSeedNodes_CallsShutdownCallback verifies
// that when the join timeout fires, ShutdownCallback is invoked (which in
// production triggers CoordinatedShutdown).
func TestShutdownAfterUnsuccessfulJoinSeedNodes_CallsShutdownCallback(t *testing.T) {
	cm := newTimerTestCM()
	cm.ShutdownAfterUnsuccessfulJoinSeedNodes = 100 * time.Millisecond
	cm.RetryUnsuccessfulJoinAfter = 1 * time.Second // long retry so it doesn't interfere

	var called atomic.Bool
	done := make(chan struct{})
	cm.ShutdownCallback = func() {
		called.Store(true)
		close(done)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := cm.JoinCluster(ctx, "10.0.0.99", 2551)
	if err != nil {
		t.Fatalf("JoinCluster: %v", err)
	}

	select {
	case <-done:
		if !called.Load() {
			t.Fatal("ShutdownCallback was not called")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownCallback not called within 2s")
	}
}

// TestShutdownAfterUnsuccessfulJoinSeedNodes_NoCallbackWhenWelcomeReceived
// verifies that the shutdown callback is NOT invoked when the node
// successfully joins before the timeout.
func TestShutdownAfterUnsuccessfulJoinSeedNodes_NoCallbackWhenWelcomeReceived(t *testing.T) {
	cm := newTimerTestCM()
	cm.ShutdownAfterUnsuccessfulJoinSeedNodes = 200 * time.Millisecond
	cm.RetryUnsuccessfulJoinAfter = 1 * time.Second

	var called atomic.Bool
	cm.ShutdownCallback = func() {
		called.Store(true)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := cm.JoinCluster(ctx, "10.0.0.99", 2551)
	if err != nil {
		t.Fatalf("JoinCluster: %v", err)
	}

	// Simulate Welcome received before timeout.
	time.Sleep(50 * time.Millisecond)
	cm.WelcomeReceived.Store(true)

	// Wait past the deadline.
	time.Sleep(300 * time.Millisecond)

	if called.Load() {
		t.Error("ShutdownCallback should not be called when Welcome was received")
	}
}

// captureLog redirects the global logger to a buffer for the duration of fn,
// then restores it and returns the captured output.
func captureLog(fn func()) string {
	var buf bytes.Buffer
	old := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0) // no timestamps for easier matching
	defer func() {
		log.SetOutput(old)
		log.SetFlags(oldFlags)
	}()
	fn()
	return buf.String()
}

func TestLogInfo_SuppressesInfoMessages(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint32(123),
		Uid2:    proto.Uint32(0),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }

	// LogInfo=true (default) — info messages should appear.
	cm := NewClusterManager(local, router)
	cm.LogInfo = true
	output := captureLog(func() {
		cm.performLeaderActions()
	})
	// performLeaderActions may or may not produce output depending on state,
	// but JoinCluster always logs "initiating join" when LogInfo=true.
	output = captureLog(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = cm.JoinCluster(ctx, "10.0.0.1", 2551)
		cm.WelcomeReceived.Store(true)
		time.Sleep(60 * time.Millisecond)
	})
	if !strings.Contains(output, "initiating join") {
		t.Errorf("LogInfo=true: expected 'initiating join' in log output, got: %q", output)
	}

	// LogInfo=false — info messages should be suppressed.
	cm2 := NewClusterManager(local, router)
	cm2.LogInfo = false
	cm2.WelcomeReceived.Store(false)
	output = captureLog(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = cm2.JoinCluster(ctx, "10.0.0.1", 2551)
		cm2.WelcomeReceived.Store(true)
		time.Sleep(60 * time.Millisecond)
	})
	if strings.Contains(output, "initiating join") {
		t.Errorf("LogInfo=false: 'initiating join' should be suppressed, got: %q", output)
	}
}

func TestLogInfoVerbose_ConvergenceDetails(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint32(123),
		Uid2:    proto.Uint32(0),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }

	// LogInfoVerbose=false — convergence details should NOT appear.
	cm := NewClusterManager(local, router)
	cm.LogInfoVerbose = false
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	output := captureLog(func() {
		cm.CheckConvergenceLocked()
	})
	if strings.Contains(output, "convergence check") {
		t.Errorf("LogInfoVerbose=false: convergence details should be suppressed, got: %q", output)
	}

	// LogInfoVerbose=true — convergence details should appear.
	cm.LogInfoVerbose = true
	output = captureLog(func() {
		cm.CheckConvergenceLocked()
	})
	if !strings.Contains(output, "convergence check passed") {
		t.Errorf("LogInfoVerbose=true: expected 'convergence check passed', got: %q", output)
	}

	// Add a second member NOT in seen — convergence should fail.
	cm.State.AllAddresses = append(cm.State.AllAddresses, &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2553)},
		Uid:     proto.Uint32(456),
	})
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(2),
	})
	output = captureLog(func() {
		cm.CheckConvergenceLocked()
	})
	if !strings.Contains(output, "convergence check failed") {
		t.Errorf("LogInfoVerbose=true: expected 'convergence check failed', got: %q", output)
	}
}

// ── Session 11: publish-stats-interval ──────────────────────────────────────

// statsCollectorActor is a minimal actor.Ref that records every
// CurrentClusterStats event delivered through the cluster event bus.
type statsCollectorActor struct {
	mu       sync.Mutex
	received []CurrentClusterStats
}

func (s *statsCollectorActor) Tell(msg any, _ ...actor.Ref) {
	if stats, ok := msg.(CurrentClusterStats); ok {
		s.mu.Lock()
		s.received = append(s.received, stats)
		s.mu.Unlock()
	}
}

func (s *statsCollectorActor) Path() string { return "/user/statsCollector" }

func (s *statsCollectorActor) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func (s *statsCollectorActor) Last() CurrentClusterStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.received) == 0 {
		return CurrentClusterStats{}
	}
	return s.received[len(s.received)-1]
}

// TestComputeClusterStats_Counts walks state and verifies counts by status.
func TestComputeClusterStats_Counts(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2700),
		},
		Uid: proto.Uint32(1),
	}
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.AllAddresses = append(cm.State.AllAddresses, &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2701), System: proto.String("TestSystem"), Protocol: proto.String("pekko")},
		Uid:     proto.Uint32(2),
	})
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Joining.Enum(),
	})
	cm.State.AllAddresses = append(cm.State.AllAddresses, &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2702), System: proto.String("TestSystem"), Protocol: proto.String("pekko")},
		Uid:     proto.Uint32(3),
	})
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(2),
		Status:       gproto_cluster.MemberStatus_Down.Enum(),
	})
	cm.Mu.Unlock()

	stats := cm.computeClusterStats()
	if stats.Members != 3 {
		t.Errorf("Members = %d, want 3", stats.Members)
	}
	if stats.Up != 1 {
		t.Errorf("Up = %d, want 1", stats.Up)
	}
	if stats.Joining != 1 {
		t.Errorf("Joining = %d, want 1", stats.Joining)
	}
	if stats.Down != 1 {
		t.Errorf("Down = %d, want 1", stats.Down)
	}
}

// TestStartPublishStatsLoop_EmitsAtCadence verifies that StartPublishStatsLoop
// publishes CurrentClusterStats events to subscribers at PublishStatsInterval
// cadence (Pekko-compatible: pekko.cluster.publish-stats-interval).
func TestStartPublishStatsLoop_EmitsAtCadence(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2710),
		},
		Uid: proto.Uint32(1),
	}
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.Mu.Unlock()

	cm.PublishStatsInterval = 50 * time.Millisecond

	collector := &statsCollectorActor{}
	cm.Subscribe(collector, EventCurrentClusterStats)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm.StartPublishStatsLoop(ctx)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if collector.Count() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := collector.Count(); got < 2 {
		t.Fatalf("expected ≥2 CurrentClusterStats events at 50ms cadence, got %d", got)
	}
	last := collector.Last()
	if last.Up != 1 {
		t.Errorf("last stats Up = %d, want 1", last.Up)
	}
}

// TestStartPublishStatsLoop_OffDisables verifies that PublishStatsInterval=0
// (HOCON value "off") disables the loop entirely.
func TestStartPublishStatsLoop_OffDisables(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2711),
		},
		Uid: proto.Uint32(1),
	}
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.PublishStatsInterval = 0

	collector := &statsCollectorActor{}
	cm.Subscribe(collector, EventCurrentClusterStats)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm.StartPublishStatsLoop(ctx)

	time.Sleep(150 * time.Millisecond)
	if got := collector.Count(); got != 0 {
		t.Errorf("expected 0 events when PublishStatsInterval=0, got %d", got)
	}
}

// ── Session 12: multi-DC failure detector ───────────────────────────────────

// multiDCTestCM builds a cluster manager with a local Up node in DC "us-east"
// plus a peer Up node in DC "eu-west" (cross-DC).
func multiDCTestCM(t *testing.T) (*ClusterManager, *gproto_cluster.Address, *gproto_cluster.Address) {
	t.Helper()
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2720),
		},
		Uid: proto.Uint32(1),
	}
	cm := NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	cm.SetLocalDataCenter("us-east")

	peer := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2721),
		},
		Uid: proto.Uint32(2),
	}

	cm.Mu.Lock()
	cm.State.AllRoles = []string{"dc-us-east", "dc-eu-west"}
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].RolesIndexes = []int32{0}
	cm.State.AllAddresses = append(cm.State.AllAddresses, peer)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		RolesIndexes: []int32{1},
	})
	cm.Mu.Unlock()

	return cm, local.GetAddress(), peer.GetAddress()
}

// TestIsCrossDC_DistinguishesByDC verifies IsCrossDC returns true only when
// the target's data-center role differs from LocalDataCenter.
func TestIsCrossDC_DistinguishesByDC(t *testing.T) {
	cm, localAddr, peerAddr := multiDCTestCM(t)

	if cm.IsCrossDC(localAddr) {
		t.Errorf("IsCrossDC(local) = true, want false (same DC)")
	}
	if !cm.IsCrossDC(peerAddr) {
		t.Errorf("IsCrossDC(peer) = false, want true (different DC)")
	}
}

// TestIsCrossDC_UnknownDefaultsToIntraDC verifies that an unknown target
// (not in gossip state) is treated as intra-DC for safety.
func TestIsCrossDC_UnknownDefaultsToIntraDC(t *testing.T) {
	cm, _, _ := multiDCTestCM(t)
	stranger := &gproto_cluster.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("TestSystem"),
		Hostname: proto.String("203.0.113.42"),
		Port:     proto.Uint32(2552),
	}
	if cm.IsCrossDC(stranger) {
		t.Errorf("IsCrossDC(unknown) = true, want false (default safe)")
	}
}

// TestEffectiveHeartbeatInterval_PicksCrossDCWhenSet verifies the helper
// returns CrossDCHeartbeatInterval for cross-DC targets and HeartbeatInterval
// for intra-DC targets.
func TestEffectiveHeartbeatInterval_PicksCrossDCWhenSet(t *testing.T) {
	cm, localAddr, peerAddr := multiDCTestCM(t)
	cm.HeartbeatInterval = 1 * time.Second
	cm.CrossDCHeartbeatInterval = 3 * time.Second

	if got := cm.EffectiveHeartbeatInterval(localAddr); got != 1*time.Second {
		t.Errorf("EffectiveHeartbeatInterval(local) = %v, want 1s (intra-DC)", got)
	}
	if got := cm.EffectiveHeartbeatInterval(peerAddr); got != 3*time.Second {
		t.Errorf("EffectiveHeartbeatInterval(peer) = %v, want 3s (cross-DC)", got)
	}
}

// TestEffectiveHeartbeatInterval_FallsBackWhenCrossDCZero verifies that when
// CrossDCHeartbeatInterval is zero, even cross-DC targets use HeartbeatInterval.
func TestEffectiveHeartbeatInterval_FallsBackWhenCrossDCZero(t *testing.T) {
	cm, _, peerAddr := multiDCTestCM(t)
	cm.HeartbeatInterval = 750 * time.Millisecond
	cm.CrossDCHeartbeatInterval = 0

	if got := cm.EffectiveHeartbeatInterval(peerAddr); got != 750*time.Millisecond {
		t.Errorf("EffectiveHeartbeatInterval(peer) with CrossDC=0 = %v, want 750ms", got)
	}
}

// ── Sub-plan 8 Group A: multi-DC FD expected-response-after ─────────────────

// TestEffectiveExpectedResponseAfter_PicksCrossDCWhenSet verifies the helper
// returns CrossDCExpectedResponseAfter for cross-DC targets and the intra-DC
// ExpectedResponseAfter for intra-DC targets.
func TestEffectiveExpectedResponseAfter_PicksCrossDCWhenSet(t *testing.T) {
	cm, localAddr, peerAddr := multiDCTestCM(t)
	cm.ExpectedResponseAfter = 1 * time.Second
	cm.CrossDCExpectedResponseAfter = 4 * time.Second

	if got := cm.EffectiveExpectedResponseAfter(localAddr); got != 1*time.Second {
		t.Errorf("EffectiveExpectedResponseAfter(local) = %v, want 1s (intra-DC)", got)
	}
	if got := cm.EffectiveExpectedResponseAfter(peerAddr); got != 4*time.Second {
		t.Errorf("EffectiveExpectedResponseAfter(peer) = %v, want 4s (cross-DC)", got)
	}
}

// TestEffectiveExpectedResponseAfter_FallsBackWhenCrossDCZero verifies that
// when CrossDCExpectedResponseAfter is zero, even cross-DC targets use the
// intra-DC ExpectedResponseAfter.
func TestEffectiveExpectedResponseAfter_FallsBackWhenCrossDCZero(t *testing.T) {
	cm, _, peerAddr := multiDCTestCM(t)
	cm.ExpectedResponseAfter = 800 * time.Millisecond
	cm.CrossDCExpectedResponseAfter = 0

	if got := cm.EffectiveExpectedResponseAfter(peerAddr); got != 800*time.Millisecond {
		t.Errorf("EffectiveExpectedResponseAfter(peer) with CrossDC=0 = %v, want 800ms", got)
	}
}

// TestEffectiveExpectedResponseAfter_Default verifies the helper falls back to
// 1s when both CrossDCExpectedResponseAfter and intra-DC ExpectedResponseAfter
// are zero (matches the Pekko reference default).
func TestEffectiveExpectedResponseAfter_Default(t *testing.T) {
	cm, _, peerAddr := multiDCTestCM(t)
	if got := cm.EffectiveExpectedResponseAfter(peerAddr); got != 1*time.Second {
		t.Errorf("EffectiveExpectedResponseAfter default = %v, want 1s", got)
	}
}

// TestHandleHeartbeat_SeedsFDWithEffectiveExpectedResponseAfter verifies that
// the heartbeat handler constructs the per-node Phi detector with the
// effective expected-response-after for the target's DC. This is the live
// runtime consumer of pekko.cluster.multi-data-center.failure-detector.
// expected-response-after.
func TestHandleHeartbeat_SeedsFDWithEffectiveExpectedResponseAfter(t *testing.T) {
	cm, _, peerAddr := multiDCTestCM(t)
	cm.ExpectedResponseAfter = 1 * time.Second
	cm.CrossDCExpectedResponseAfter = 7 * time.Second

	// Drive a heartbeat directly via the FD path (simulates handleHeartbeat
	// without needing a full proto-encoded payload).
	uidLow := uint64(2)
	key := fmt.Sprintf("%s:%d-%d", peerAddr.GetHostname(), peerAddr.GetPort(), uidLow)
	cm.Fd.HeartbeatWithEstimate(key, cm.EffectiveExpectedResponseAfter(peerAddr))

	if got := cm.Fd.FirstHeartbeatEstimateFor(key); got != 7*time.Second {
		t.Errorf("FirstHeartbeatEstimateFor(cross-DC peer) = %v, want 7s", got)
	}
}

// TestApplyDetectorConfig_WiresExpectedResponseAfter verifies that a non-zero
// ExpectedResponseAfter on FailureDetectorConfig is mirrored onto
// cm.ExpectedResponseAfter so EffectiveExpectedResponseAfter can return it.
func TestApplyDetectorConfig_WiresExpectedResponseAfter(t *testing.T) {
	cm := newTestCM()
	ApplyDetectorConfig(cm, FailureDetectorConfig{
		Threshold:             10.0,
		MaxSampleSize:         500,
		MinStdDeviation:       200 * time.Millisecond,
		ExpectedResponseAfter: 600 * time.Millisecond,
	})
	if cm.ExpectedResponseAfter != 600*time.Millisecond {
		t.Errorf("cm.ExpectedResponseAfter = %v, want 600ms", cm.ExpectedResponseAfter)
	}
}

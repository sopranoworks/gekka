/*
 * cluster_singleton_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// ── minimal actor for singletonProps ─────────────────────────────────────────

type noopSingletonActor struct{ actor.BaseActor }

func (n *noopSingletonActor) Receive(msg any) {}

// ── minimal test infrastructure ───────────────────────────────────────────────

// singletonTestRef is a minimal actor.Ref that records Tell calls.
type singletonTestRef struct {
	mu      sync.Mutex
	path    string
	msgs    []any
	stopped atomic.Bool
}

func (r *singletonTestRef) Path() string { return r.path }
func (r *singletonTestRef) Tell(msg any, sender ...actor.Ref) {
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()
}

// singletonTestContext records ActorOf and Stop calls for assertions.
type singletonTestContext struct {
	mu      sync.Mutex
	spawned []*singletonTestRef
	stopped []*singletonTestRef
	watches [][2]string
}

func (c *singletonTestContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	ref := &singletonTestRef{path: "/user/" + name}
	c.mu.Lock()
	c.spawned = append(c.spawned, ref)
	c.mu.Unlock()
	return ref, nil
}

func (c *singletonTestContext) Stop(ref actor.Ref) {
	if r, ok := ref.(*singletonTestRef); ok {
		r.stopped.Store(true)
		c.mu.Lock()
		c.stopped = append(c.stopped, r)
		c.mu.Unlock()
	}
}

func (c *singletonTestContext) Watch(watcher actor.Ref, target actor.Ref) {
	c.mu.Lock()
	c.watches = append(c.watches, [2]string{watcher.Path(), target.Path()})
	c.mu.Unlock()
}

func (c *singletonTestContext) Resolve(path string) (actor.Ref, error) {
	return &singletonTestRef{path: path}, nil
}

func (c *singletonTestContext) ActorSelection(path string) actor.ActorSelection {
	return actor.ActorSelection{
		Anchor: &singletonTestRef{path: "/"},
		Path:   actor.ParseSelectionElements(path),
		System: c,
	}
}

func (c *singletonTestContext) DeliverSelection(s actor.ActorSelection, msg any, sender ...actor.Ref) {
}

func (c *singletonTestContext) ResolveSelection(s actor.ActorSelection, ctx context.Context) (actor.Ref, error) {
	return &singletonTestRef{path: "/test"}, nil
}

func (c *singletonTestContext) AskSelection(s actor.ActorSelection, ctx context.Context, msg any) (any, error) {
	return nil, nil
}

func (c *singletonTestContext) Context() context.Context {
	return context.Background()
}

func (c *singletonTestContext) Spawn(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}

func (c *singletonTestContext) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return nil, nil
}

func (c *singletonTestContext) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return nil, nil
}

func (c *singletonTestContext) spawnCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spawned)
}

func (c *singletonTestContext) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.stopped)
}

// ── helper: build a ClusterManager with a single local Up node ───────────────

func newSingletonTestCM(host string, port uint32, upNumber int32) *cluster.ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String(host),
			Port:     proto.Uint32(port),
		},
		Uid: proto.Uint32(1),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }
	cm := cluster.NewClusterManager(local, router)
	// Promote the node to Up with the given upNumber.
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(upNumber)
	return cm
}

// newWiredManager builds a ClusterSingletonManager with the test context injected.
func newWiredManager(cm *cluster.ClusterManager, ctx *singletonTestContext) *ClusterSingletonManager {
	props := actor.Props{New: func() actor.Actor { return &noopSingletonActor{} }}
	mgr := NewClusterSingletonManager(cm, props, "")
	selfRef := &singletonTestRef{path: "/user/singletonManager"}
	// Inject self reference (same pattern used in pool_router_test.go).
	type selfSetter interface{ SetSelf(actor.Ref) }
	if ss, ok := any(mgr).(selfSetter); ok {
		ss.SetSelf(selfRef)
	}
	actor.InjectSystem(mgr, ctx)
	return mgr
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestSingletonManager_RoleFromConstructor verifies that the role parameter
// is stored and accessible, matching HOCON pekko.cluster.singleton.role flow.
func TestSingletonManager_RoleFromConstructor(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	props := actor.Props{New: func() actor.Actor { return &noopSingletonActor{} }}

	mgr := NewClusterSingletonManager(cm, props, "backend")
	if mgr.Role() != "backend" {
		t.Errorf("Role() = %q, want %q", mgr.Role(), "backend")
	}

	mgr2 := NewClusterSingletonManager(cm, props, "")
	if mgr2.Role() != "" {
		t.Errorf("Role() = %q, want empty", mgr2.Role())
	}
}

// TestSingletonManager_HandOverRetryInterval verifies the default and
// configured hand-over-retry-interval, matching HOCON flow.
func TestSingletonManager_HandOverRetryInterval(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	props := actor.Props{New: func() actor.Actor { return &noopSingletonActor{} }}

	// Default is 1s.
	mgr := NewClusterSingletonManager(cm, props, "")
	if mgr.HandOverRetryInterval() != 1*time.Second {
		t.Errorf("HandOverRetryInterval() = %v, want 1s", mgr.HandOverRetryInterval())
	}

	// WithHandOverRetryInterval overrides.
	mgr.WithHandOverRetryInterval(3 * time.Second)
	if mgr.HandOverRetryInterval() != 3*time.Second {
		t.Errorf("HandOverRetryInterval() = %v, want 3s", mgr.HandOverRetryInterval())
	}

	// Zero is ignored (keeps previous value).
	mgr.WithHandOverRetryInterval(0)
	if mgr.HandOverRetryInterval() != 3*time.Second {
		t.Errorf("HandOverRetryInterval() = %v after zero, want 3s (unchanged)", mgr.HandOverRetryInterval())
	}
}

// TestSingletonManager_SpawnsWhenOldest verifies that PreStart spawns the
// singleton immediately when the local node is the only Up member.
func TestSingletonManager_SpawnsWhenOldest(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	mgr.PreStart()

	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn, got %d", ctx.spawnCount())
	}
	if len(ctx.watches) != 1 {
		t.Fatalf("expected 1 watch registration, got %d", len(ctx.watches))
	}
}

// TestSingletonManager_NoSpawnWhenNotOldest verifies that the manager does NOT
// spawn the singleton when a different node has a lower upNumber.
func TestSingletonManager_NoSpawnWhenNotOldest(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2553, 1) // local = upNumber 1

	// Add an older node (upNumber 0) to the gossip state.
	older := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(2),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, older)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(0),
	})

	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	mgr.PreStart()

	if ctx.spawnCount() != 0 {
		t.Fatalf("expected 0 spawns (not oldest), got %d", ctx.spawnCount())
	}
}

// TestSingletonManager_StopsOnLeadershipLoss verifies that the singleton is
// stopped when a membership event causes another node to become the oldest.
func TestSingletonManager_StopsOnLeadershipLoss(t *testing.T) {
	// Local node starts as the only Up member (oldest).
	cm := newSingletonTestCM("127.0.0.1", 2553, 1)
	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	// PreStart: local is oldest → spawn singleton.
	mgr.PreStart()
	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn after PreStart, got %d", ctx.spawnCount())
	}

	// Simulate a new node joining with a lower upNumber (becomes new oldest).
	newOldest := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(2),
	}
	cm.Mu.Lock()
	cm.State.AllAddresses = append(cm.State.AllAddresses, newOldest)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(0),
	})
	cm.Mu.Unlock()

	// Deliver a MemberUp event to trigger re-evaluation.
	mgr.Receive(cluster.MemberUp{Member: cluster.MemberAddress{Host: "127.0.0.1", Port: 2552}})

	if ctx.stopCount() != 1 {
		t.Fatalf("expected 1 stop after leadership loss, got %d", ctx.stopCount())
	}
}

// TestSingletonManager_RespawnsAfterBecomingOldest verifies that if the
// singleton was stopped (leadership elsewhere) and then the local node becomes
// oldest again (e.g. other node left), it re-spawns.
func TestSingletonManager_RespawnsAfterBecomingOldest(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2553, 1)

	// Add an older node.
	older := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(2),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, older)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(0),
	})

	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	// PreStart: older node exists → no spawn.
	mgr.PreStart()
	if ctx.spawnCount() != 0 {
		t.Fatalf("expected 0 spawns initially, got %d", ctx.spawnCount())
	}

	// Older node leaves → remove it from state.
	cm.Mu.Lock()
	cm.State.AllAddresses = cm.State.AllAddresses[:1]
	cm.State.Members = cm.State.Members[:1]
	cm.State.Members[0].UpNumber = proto.Int32(0)
	cm.Mu.Unlock()

	mgr.Receive(cluster.MemberRemoved{Member: cluster.MemberAddress{Host: "127.0.0.1", Port: 2552}})

	// maybeSpawnOrStop is synchronous; the singleton should be spawned immediately.
	deadline := time.Now().Add(time.Second)
	for ctx.spawnCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn after becoming oldest, got %d", ctx.spawnCount())
	}
}

// TestSingletonManager_PostStopStopsSingleton verifies that PostStop stops the
// singleton if it is running locally.
func TestSingletonManager_PostStopStopsSingleton(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	mgr.PreStart()
	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn, got %d", ctx.spawnCount())
	}

	mgr.PostStop()
	if ctx.stopCount() != 1 {
		t.Fatalf("expected 1 stop on PostStop, got %d", ctx.stopCount())
	}
}

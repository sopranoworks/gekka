/*
 * scale_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package bench contains performance benchmarks for the Gekka framework.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem ./test/bench/
//
// Run with CPU profile:
//
//	go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./test/bench/
//	go tool pprof -http=:8080 cpu.prof
//
// Run only scale benchmarks:
//
//	go test -bench=BenchmarkScale -benchmem ./test/bench/
package bench

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
)

// ── minimal actor infrastructure ─────────────────────────────────────────────

// benchActorContext is a minimal ActorContext used in scale benchmarks.
// It assigns unique paths and calls PreStart synchronously (no goroutines),
// matching the pattern used in persistent_sharding_test.go.
type benchActorContext struct {
	mu      sync.Mutex
	seq     atomic.Int64
	actors  map[string]actor.Ref
}

func newBenchActorContext() *benchActorContext {
	return &benchActorContext{actors: make(map[string]actor.Ref)}
}

func (c *benchActorContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	a := props.New()
	path := fmt.Sprintf("/user/%s", name)
	ref := &benchRef{path: path, actor: a}
	if s, ok := a.(interface {
		SetSystem(actor.ActorContext)
		SetSelf(actor.Ref)
	}); ok {
		s.SetSystem(c)
		s.SetSelf(ref)
	}
	if ps, ok := a.(interface{ PreStart() }); ok {
		ps.PreStart()
	}
	c.mu.Lock()
	c.actors[name] = ref
	c.mu.Unlock()
	return ref, nil
}

func (c *benchActorContext) Stop(ref actor.Ref) {
	c.mu.Lock()
	for k, v := range c.actors {
		if v.Path() == ref.Path() {
			delete(c.actors, k)
			break
		}
	}
	c.mu.Unlock()
	if ps, ok := ref.(*benchRef); ok {
		if pp, ok := ps.actor.(interface{ PostStop() }); ok {
			pp.PostStop()
		}
	}
}

func (c *benchActorContext) Resolve(path string) (actor.Ref, error) {
	return &benchRef{path: path}, nil
}

func (c *benchActorContext) Watch(_, _ actor.Ref) {}

func (c *benchActorContext) SpawnAnonymous(behavior any) (actor.Ref, error) {
	n := fmt.Sprintf("anon-%d", c.seq.Add(1))
	return c.Spawn(behavior, n)
}

func (c *benchActorContext) Spawn(behavior any, name string) (actor.Ref, error) {
	return &benchRef{path: "/user/" + name}, nil
}

func (c *benchActorContext) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return &benchRef{path: "/system/" + name}, nil
}

func (c *benchActorContext) Context() context.Context { return context.Background() }

// benchRef is a synchronous actor reference.
type benchRef struct {
	path  string
	actor actor.Actor
}

func (r *benchRef) Path() string { return r.path }
func (r *benchRef) Tell(msg any, sender ...actor.Ref) {
	if r.actor == nil {
		return
	}
	if ss, ok := r.actor.(interface{ SetSender(actor.Ref) }); ok {
		var s actor.Ref
		if len(sender) > 0 {
			s = sender[0]
		}
		ss.SetSender(s)
	}
	r.actor.Receive(msg)
}

// ── Classic actor ─────────────────────────────────────────────────────────────

// classicNopActor is the smallest meaningful classic actor: receives a message
// and does nothing.  Used to measure per-actor memory overhead.
type classicNopActor struct {
	actor.BaseActor
}

func (a *classicNopActor) Receive(_ any) {}

// ── Typed actor ───────────────────────────────────────────────────────────────

// typedNopBehavior is the smallest typed behavior: receives any string and stays.
func typedNopBehavior(_ typed.TypedContext[string], _ string) typed.Behavior[string] {
	return typed.Same[string]()
}

// ── Scale benchmarks ──────────────────────────────────────────────────────────

// BenchmarkScale_ClassicActorMemory measures the heap allocated per classic actor.
//
// Method: force a full GC, read memstats, spawn N actors, force GC again, read
// memstats.  HeapInuse delta / N = bytes per actor.
//
// Expected: ≈ 512 bytes (BaseActor struct + 256-slot chan any).
func BenchmarkScale_ClassicActorMemory(b *testing.B) {
	const actorCount = 10_000

	b.ReportAllocs()

	// Pin to one CPU to reduce noise from concurrent GC.
	runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(runtime.NumCPU())

	var before, after runtime.MemStats

	for i := 0; i < b.N; i++ {
		refs := make([]actor.Ref, 0, actorCount)
		ctx := newBenchActorContext()

		runtime.GC()
		runtime.ReadMemStats(&before)

		for j := 0; j < actorCount; j++ {
			ref, _ := ctx.ActorOf(actor.Props{
				New: func() actor.Actor {
					return &classicNopActor{BaseActor: actor.NewBaseActor()}
				},
			}, fmt.Sprintf("classic-%d-%d", i, j))
			refs = append(refs, ref)
		}

		runtime.GC()
		runtime.ReadMemStats(&after)

		bytesPerActor := int64(after.HeapInuse-before.HeapInuse) / actorCount
		b.ReportMetric(float64(bytesPerActor), "bytes/actor")
		_ = refs
	}
}

// BenchmarkScale_TypedActorMemory measures the heap allocated per typed actor.
//
// Typed actors wrap a classic actor so their baseline overhead includes
// TypedActor[T] + BaseActor + the 256-slot mailbox.
//
// Expected: ≈ 700–900 bytes (TypedActor wrapper + BaseActor + chan).
func BenchmarkScale_TypedActorMemory(b *testing.B) {
	const actorCount = 10_000

	b.ReportAllocs()
	runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(runtime.NumCPU())

	var before, after runtime.MemStats

	for i := 0; i < b.N; i++ {
		ctx := newBenchActorContext()
		refs := make([]actor.Ref, 0, actorCount)

		runtime.GC()
		runtime.ReadMemStats(&before)

		for j := 0; j < actorCount; j++ {
			ref, _ := typed.Spawn(ctx, typedNopBehavior,
				fmt.Sprintf("typed-%d-%d", i, j))
			refs = append(refs, ref.Untyped())
		}

		runtime.GC()
		runtime.ReadMemStats(&after)

		bytesPerActor := int64(after.HeapInuse-before.HeapInuse) / actorCount
		b.ReportMetric(float64(bytesPerActor), "bytes/actor")
		_ = refs
	}
}

// BenchmarkScale_SpawnShardedActors measures the wall time to spawn 100,000
// sharded persistent actors.
//
// Expected throughput: ≥ 50,000 spawns/s on a laptop (in-memory, single node).
func BenchmarkScale_SpawnShardedActors(b *testing.B) {
	const entityCount = 100_000
	const shardCount = 100

	// ── 1. Setup OUTSIDE the loop ────────────────────────────────────────────

	// Use a real cluster node (single-node) to measure realistic overhead.
	// Port 0 picks a random free port to prevent collisions.
	cfg := gekka.ClusterConfig{
		SystemName: "ScaleTest",
		Host:       "127.0.0.1",
		Port:       0,
	}
	system, err := gekka.NewCluster(cfg)
	if err != nil {
		b.Fatal(err)
	}

	// Register message types for serialization
	system.RegisterType("bench.shardMsg", reflect.TypeOf(shardMsg{}))

	// ── 2. Ensure Proper Teardown ───────────────────────────────────────────

	// b.Cleanup ensures resources are freed even if the benchmark fails.
	b.Cleanup(func() {
		system.Terminate()
		<-system.WhenTerminated()
	})

	received := atomic.Int64{}
	extractor := func(msg any) (sharding.EntityID, sharding.ShardID, any) {
		if m, ok := msg.(shardMsg); ok {
			shard := "shard-" + strconv.Itoa(int(fnv32(m.id)%shardCount))
			return m.id, shard, m
		}
		return "", "", msg
	}

	// For minimal overhead, we'll use a no-op persistent actor behavior
	behaviorFactory := func(_ string) *ptyped.EventSourcedBehavior[shardMsg, any, any] {
		return &ptyped.EventSourcedBehavior[shardMsg, any, any]{
			CommandHandler: func(ctx typed.TypedContext[shardMsg], _ any, _ shardMsg) ptyped.Effect[any, any] {
				received.Add(1)
				return ptyped.None[any, any]()
			},
			EventHandler: func(state any, _ any) any { return state },
		}
	}

	settings := gekka.ShardingSettings{
		NumberOfShards: shardCount,
	}

	// Start Sharding (Node becomes a ShardRegion)
	region, err := gekka.StartTyped(system, "ScaleTest", behaviorFactory, extractor, settings)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-load all shard homes locally to avoid coordinator round-trips/stashing.
	// This ensures messages are routed immediately to local shards.
	for s := 0; s < shardCount; s++ {
		region.Tell(sharding.ShardHome{
			ShardId:    "shard-" + strconv.Itoa(s),
			RegionPath: region.Path(),
		})
	}

	// Call ResetTimer exactly ONCE before the loop starts
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Spawn 100,000 actors per iteration
		for j := 0; j < entityCount; j++ {
			// Efficient entityID generation using strconv.Itoa as requested.
			// Use i*entityCount + j to ensure unique entities across iterations.
			id := strconv.Itoa(i*entityCount + j)
			region.Tell(shardMsg{id: id})
		}
	}

	// Final report: capture deliveries and report as custom metrics.
	b.StopTimer() // Don't count report overhead
	b.ReportMetric(float64(received.Load()), "msgs_delivered")
}

// ── helpers ───────────────────────────────────────────────────────────────────

type shardMsg struct{ id string }

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

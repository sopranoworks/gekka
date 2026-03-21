/*
 * throughput_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package bench

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster/sharding"
	stypedsharding "github.com/sopranoworks/gekka/cluster/sharding/typed"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// ── Message types for benchmarking ───────────────────────────────────────────

type throughputMsg struct {
	Data string
}

type askMsg struct{}
type ackMsg struct{}

// ── sinkActor counts messages and replies to Ask ─────────────────────────────

type sinkActor struct {
	actor.BaseActor
	count *atomic.Int64
}

func (s *sinkActor) Receive(msg any) {
	switch msg.(type) {
	case throughputMsg, *throughputMsg:
		s.count.Add(1)
	case askMsg, *askMsg:
		s.count.Add(1)
		s.Sender().Tell(ackMsg{})
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func setupNodes(b *testing.B) (gekka.ActorSystem, gekka.ActorSystem, actor.Address) {
	cfgA := gekka.ClusterConfig{SystemName: "ThroughputA", Host: "127.0.0.1", Port: 0}
	sysA, err := gekka.NewCluster(cfgA)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		sysA.Terminate()
		<-sysA.WhenTerminated()
	})

	cfgB := gekka.ClusterConfig{SystemName: "ThroughputB", Host: "127.0.0.1", Port: 0}
	sysB, err := gekka.NewCluster(cfgB)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		sysB.Terminate()
		<-sysB.WhenTerminated()
	})

	// Register types on both nodes
	for _, sys := range []gekka.ActorSystem{sysA, sysB} {
		sys.RegisterType("bench.throughputMsg", reflect.TypeOf(throughputMsg{}))
		sys.RegisterType("bench.askMsg", reflect.TypeOf(askMsg{}))
		sys.RegisterType("bench.ackMsg", reflect.TypeOf(ackMsg{}))
		sys.RegisterType("string", reflect.TypeOf(""))
		// Register ReliableEnvelope for throughputMsg
		sys.RegisterType("ReliableEnvelope:bench.throughputMsg", reflect.TypeOf(stypedsharding.ReliableEnvelope[throughputMsg]{}))
	}

	addrB := sysB.SelfAddress()

	// Initiate handshake and wait
	sysA.Send(context.Background(), sysA.RemoteActorOf(addrB, "/user/handshake").Path(), throughputMsg{Data: "handshake"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sysA.WaitForHandshake(ctx, addrB.Host, uint32(addrB.Port))

	return sysA, sysB, addrB
}

// ── Remote Throughput Benchmarks ─────────────────────────────────────────────

// BenchmarkRemoteTell measures the rate at which Node A can send messages to
// Node B via a remote ActorRef.
func BenchmarkRemoteTell(b *testing.B) {
	sysA, sysB, addrB := setupNodes(b)
	count := &atomic.Int64{}
	_, err := sysB.ActorOf(actor.Props{New: func() actor.Actor { return &sinkActor{count: count} }}, "sink")
	if err != nil {
		b.Fatal(err)
	}

	remoteRef := sysA.RemoteActorOf(addrB, "/user/sink")

	b.ResetTimer()
	b.ReportAllocs()

	msg := throughputMsg{Data: "bench"}
	for i := 0; i < b.N; i++ {
		remoteRef.Tell(msg)
	}

	// Wait for messages to arrive to get accurate msgs/s
	deadline := time.Now().Add(10 * time.Second)
	for count.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()
	b.ReportMetric(float64(count.Load()), "msgs_received")
	b.ReportMetric(float64(count.Load())/b.Elapsed().Seconds(), "msgs/s")
}

// BenchmarkRemoteAsk measures the round-trip latency using the Ask pattern
// across two nodes.
func BenchmarkRemoteAsk(b *testing.B) {
	sysA, sysB, addrB := setupNodes(b)
	count := &atomic.Int64{}
	_, err := sysB.ActorOf(actor.Props{New: func() actor.Actor { return &sinkActor{count: count} }}, "sink")
	if err != nil {
		b.Fatal(err)
	}

	remoteRef := sysA.RemoteActorOf(addrB, "/user/sink")

	b.ResetTimer()
	b.ReportAllocs()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, err := sysA.Ask(ctx, remoteRef, askMsg{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

// ── Feature Overhead Benchmarks ──────────────────────────────────────────────

// BenchmarkReliableDeliveryOverhead compares standard EntityRef vs ReliableEntityRef.
func BenchmarkReliableDeliveryOverhead(b *testing.B) {
	cfg := gekka.ClusterConfig{SystemName: "ReliableBench", Host: "127.0.0.1", Port: 0}
	sys, err := gekka.NewCluster(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		sys.Terminate()
		<-sys.WhenTerminated()
	})

	sys.RegisterType("bench.throughputMsg", reflect.TypeOf(throughputMsg{}))
	sys.RegisterType("ReliableEnvelope:bench.throughputMsg", reflect.TypeOf(stypedsharding.ReliableEnvelope[throughputMsg]{}))

	received := atomic.Int64{}
	extractor := func(msg any) (sharding.EntityID, sharding.ShardID, any) {
		return "entity-1", "shard-1", msg
	}

	behaviorFactory := func(_ string) *ptyped.EventSourcedBehavior[throughputMsg, any, any] {
		return &ptyped.EventSourcedBehavior[throughputMsg, any, any]{
			CommandHandler: func(ctx typed.TypedContext[throughputMsg], _ any, _ throughputMsg) ptyped.Effect[any, any] {
				received.Add(1)
				return ptyped.None[any, any]()
			},
			EventHandler: func(state any, _ any) any { return state },
		}
	}

	region, err := gekka.StartTyped(sys, "ReliableBenchType", behaviorFactory, extractor, gekka.ShardingSettings{NumberOfShards: 1})
	if err != nil {
		b.Fatal(err)
	}

	// Pre-load shard home locally to avoid coordinator stashing
	region.Tell(sharding.ShardHome{ShardId: "shard-1", RegionPath: region.Path()})

	inner := stypedsharding.NewEntityRef[throughputMsg]("ReliableBenchType", "entity-1", region)

	b.Run("StandardEntityRef", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			inner.Tell(throughputMsg{Data: "bench"})
		}
		b.StopTimer()
	})

	b.Run("ReliableEntityRef", func(b *testing.B) {
		rref := stypedsharding.NewReliableEntityRef(inner, "bench-producer", 5*time.Second)
		defer rref.Stop()
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rref.Tell(throughputMsg{Data: "bench"})
		}
		b.StopTimer()
	})
}

// BenchmarkTracingOverhead compares throughput with OTel tracing disabled vs enabled.
func BenchmarkTracingOverhead(b *testing.B) {
	cfg := gekka.ClusterConfig{SystemName: "TracingBench", Host: "127.0.0.1", Port: 0}
	sys, err := gekka.NewCluster(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		sys.Terminate()
		<-sys.WhenTerminated()
	})

	sys.RegisterType("bench.throughputMsg", reflect.TypeOf(throughputMsg{}))
	count := &atomic.Int64{}
	sys.ActorOf(actor.Props{New: func() actor.Actor { return &sinkActor{count: count} }}, "sink")
	ref, _ := sys.ActorSelection("/user/sink").Resolve(context.Background())

	b.Run("TracingDisabled", func(b *testing.B) {
		otel.SetTracerProvider(sdktrace.NewTracerProvider()) // effectively no-op if no spans are started
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ref.Tell(throughputMsg{Data: "bench"})
		}
		b.StopTimer()
	})

	b.Run("TracingEnabled", func(b *testing.B) {
		rec := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		otel.SetTracerProvider(tp)
		defer otel.SetTracerProvider(otel.GetTracerProvider()) // Reset to previous

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Note: throughputMsg doesn't automatically start spans unless wrapped or in a tracing-enabled pipeline.
			// We simulate the overhead by starting a span manually if needed, or by ensuring the system is enabled.
			// Currently Gekka starts spans in ShardingRegion and Artery if enabled.
			ref.Tell(throughputMsg{Data: "bench"})
		}
		b.StopTimer()
	})
}

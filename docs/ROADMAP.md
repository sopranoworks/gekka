<!--
  ROADMAP.md — Gekka Project Development Roadmap
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Gekka Roadmap

## Released

### v0.8.0 (2026-03-17)
- **Cluster HTTP Management API**: REST endpoints for `/cluster/members`, `/health/alive`, `/health/ready` (Kubernetes probe compatible)
- **`gekka-cli`**: Command-line cluster management tool (`members` command, HOCON config loading)
- **`gekka-metrics`**: Dedicated metrics node with native OpenTelemetry export
- **Coordinated Shutdown refinement**: Phased exit sequence with readiness gate and shard handoff timeout (configurable via HOCON)
- **Rolling Update Optimization**: Operational suite for zero-downtime cluster upgrades
- **Pekko 1.1.2 Upgrade**: Verified interoperability; DC-scoped leader election for multi-DC Go nodes

### v0.7.0 (2026-03-16)
- Split Brain Resolver: `keep-majority`, `keep-oldest`, `keep-referee`, `static-quorum`
- Multi-Data Center support: DC-role gossip, DC-scoped singletons, DC-aware sharding
- Advanced Sharding: Entity Passivation (`PassivationIdleTimeout`) and Remember Entities
- SQL Persistence Backend (PostgreSQL, driver-agnostic)
- OpenTelemetry integration: tracing and metrics with W3C TraceContext propagation

### v0.6.0 (2026-03-14)
- Distributed Pub/Sub (Full Pekko compatibility, GZIP support)
- Artery TLS Transport (Secure encrypted cluster traffic)
- Reliable Delivery (Serializer ID 36, at-least-once delivery)
- Cluster Singleton Manager & Proxy (Failover support)
- Coordinated Shutdown (Phased node exit sequence)

### v0.5.0 (2026-03-13)

---

## Upcoming

### v0.9.0 — Streams, TUI & Cloud-Native Scaling

**Target:** Q3 2026

#### 1. Akka Typed API Refinement
Closing the gap with Pekko/Akka's typed actor API:
- **Timers**: `TimerScheduler` for recurring and single-shot messages inside typed actors
- **Stash**: `StashBuffer` for deferring messages until a behavior transition

#### 2. Gekka Streams
A reactive streams implementation aligned with the Akka Streams programming model:
- Source / Flow / Sink pipeline DSL
- Backpressure-aware async stages
- Integration with actor `Source.actorRef` and `Sink.actorRef`

#### 3. Kubernetes Discovery
Native cluster formation without a pre-configured seed list:
- **Kubernetes API discovery**: query the K8s API server for pod endpoints
- **DNS SRV discovery**: headless-service SRV record resolution for seed-node bootstrap
- HOCON configuration: `pekko.discovery.method = kubernetes-api | akka-dns`

#### 4. Zero-copy Serialization
Protocol-level optimization to reduce allocation pressure on the hot path:
- Custom frame builder avoiding intermediate `[]byte` copies
- Pluggable codec interface for user-defined zero-copy serializers
- Benchmarks validating throughput improvement vs. current JSON/proto path

#### 5. `gekka-cli` Dashboard (TUI)
Visual cluster management terminal UI built on top of the existing `gekka-cli`:
- Real-time member state table (role, status, DC, uptime)
- Shard distribution heatmap per region
- Log tail panel with severity filtering
- Interactive commands: `leave`, `down`, `join`

---

## Compatibility Commitment

All v0.x releases maintain binary wire compatibility with **Apache Pekko 1.x** and
**Lightbend Akka 2.6+** for the Artery TCP transport layer.

> **MANDATORY RULE (all future features):** Any network-related feature MUST include
> binary compatibility tests with Scala (Pekko/Akka) from the initial implementation phase.
> No network feature may be merged without a passing `//go:build integration` test that
> exercises the wire format against a live Scala node.

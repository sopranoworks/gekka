<!--
  ROADMAP.md — Gekka Project Development Roadmap
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Gekka Roadmap

## Released

### v0.9.0 (2026-03-18)
- **Akka Typed API Refinement**: Integrated **Timers** (`TimerScheduler`) and **Stash** (`StashBuffer`) for full functional parity with Pekko/Akka's typed actor model.
- **Gekka Streams**: Reactive streams implementation aligned with the Akka Streams programming model, featuring backpressure-aware async stages and actor integration.
- **Kubernetes-native Discovery**: Automated cluster formation using the **Kubernetes API** or **DNS SRV** (headless services) for dynamic seed node resolution.
- **Zero-copy Serialization**: High-performance Artery transport framing using `net.Buffers`, delivering **8.5x faster** throughput and zero allocations on the hot path.
- **`gekka-cli` / `gekka-metrics` TUI**: Enhanced operational tools with interactive terminal dashboards and unified Nebula/Forest branding.

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

### v0.10.0 — Finite State Machines & Structural Alignment (2026-03-19)
- **Typed FSM DSL**: Integrated behavior-based FSM for Typed Actors with full lifecycle and timer support.
- **Classic FSM Parity**: Ported the FSM DSL to Classic Actors (`BaseFSM`) for managing complex state transitions.
- **Structural Refactoring**: Relocated Sharding, Cluster Singleton, Distributed Data, and Reliable Delivery into specialized subpackages (`cluster/sharding`, `cluster/singleton`, etc.) for architectural parity with Pekko/Akka.
- **Durable State Persistence**: Added state-based persistence (`DurableStateBehavior`) as an alternative to event sourcing.
- **Advanced Routing**: Implemented Scatter-Gather, Tail-Chopping, and Consistent Hashing routing logics for both classic and typed systems.
- **Message Adaptation (Ask)**: Implemented the `TypedContext.Ask` pattern for asynchronous response handling.

### v0.11.0 — Distributed Data & Self-Healing (2026-03-20) [COMPLETED]
- **Distributed Data (CRDTs)**: Implemented `PNCounter`, `ORSet`, and `LWWRegister` with full serializer support.
- **Delta-CRDT Gossip**: Bandwidth-efficient state synchronization using delta-propagation for massive clusters.
- **Kubernetes-aware Self-Healing**: Smart Split Brain Resolver that monitors Pod lifecycle via the K8s API to accelerate recovery.

### v0.12.0 — Spanner Persistence & Performance Audit (2026-03-22) [COMPLETED]
- **Cloud Spanner Native Persistence**: Highly optimized backend using Spanner Mutations and Streaming Reads for low-latency event sourcing.
- **Advanced Sharding**: Adaptive shard allocation based on node pressure and Manual Rebalance API for ops control.
- **Exactly-once Reliable Delivery**: Deep integration between Sharding and Reliable Delivery to ensure zero message loss during failovers.
- **End-to-End Distributed Tracing**: Full OpenTelemetry instrumentation across Sharding, Persistence, and Projections.
- **Performance Benchmarking Suite**: Comprehensive suite for measuring Scale, Throughput, and Recovery metrics.

---

## Upcoming

### v0.13.0 — Documentation & Ecosystem Integration
- **Enhanced GoDoc**: Comprehensive documentation and examples across all public packages.
- **Tracing Auto-instrumentation**: Integration with popular SQL drivers (pgx, etc.) for seamless trace propagation.
- **Reliable Delivery Flow Control**: Advanced congestion control and windowing for high-throughput messaging.

### v1.0.0 — Production Readiness & Stability

**Target:** 2027

#### 1. API Stabilization
Finalize public interfaces and package structures for the first stable 1.0 release.

#### 2. Performance Tuning
Exhaustive benchmarking and optimization of the mailbox processing loop and gossip propagation.

#### 3. Documentation & Guides
Comprehensive user manual, architectural deep-dives, and production deployment best practices.

---

## Compatibility Commitment

All v0.x releases maintain binary wire compatibility with **Apache Pekko 1.x** and
**Lightbend Akka 2.6+** for the Artery TCP transport layer.

> **MANDATORY RULE (all future features):** Any network-related feature MUST include
> binary compatibility tests with Scala (Pekko/Akka) from the initial implementation phase.
> No network feature may be merged without a passing `//go:build integration` test that
> exercises the wire format against a live Scala node.

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

### v0.5.0 (2026-03-13)

---

## Upcoming

### v0.10.0 — Finite State Machines & Cluster Resilience

**Target:** Q4 2026

#### 1. (Classic) FSM API
Implementation of the Finite State Machine DSL for managing complex actor state transitions with:
- `StartWith(state, data)`
- `When(state, partialFunction)`
- `OnTransition(handler)`
- `Goto(nextState).Using(nextData)` / `Stay()`
- Integrated state-based timers

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

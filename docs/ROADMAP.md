<!--
  ROADMAP.md — Gekka Project Development Roadmap
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Gekka Roadmap

## Released

### v0.6.0 (2026-03-14)
- Distributed Pub/Sub (Full Pekko compatibility, GZIP support)
- Artery TLS Transport (Secure encrypted cluster traffic)
- Reliable Delivery (Serializer ID 36, at-least-once delivery)
- Cluster Singleton Manager & Proxy (Failover support)
- Coordinated Shutdown (Phased node exit sequence)

### v0.5.0 (2026-03-13)

---

## Upcoming

### v0.7.0 — Split Brain Resolution, Tracing & Production Database Support

**Target:** Q3 2026

#### Split Brain Resolver (SBR)
Implementation of the standard SBR strategies to handle network partitions and node crashes:
- `keep-majority`
- `keep-oldest`
- `static-quorum`

#### Distributed Tracing (`OpenTelemetry`)
Seamless integration with OpenTelemetry to provide cross-node distributed tracing across actor boundaries (especially during `Ask` patterns).

#### Production-ready Persistence Backends
Durable backends for Event Sourcing:
- **PostgreSQL** support via `database/sql`
- **SQL-based** generic drivers
- **Pebble** (LSM) embedded backend

#### Advanced Sharding Features ✅
Enhancing the `cluster/sharding` package:
- **Entity Passivation**: Automatic offloading of idle entities via `PassivationIdleTimeout` + self-initiated `Passivate`. ✅
- **Remember Entities**: Persisting `EntityStarted`/`EntityStopped` events so entities are re-spawned after Shard restart. ✅

#### Multi-DC Cluster Support
Enhanced awareness and routing for multi-datacenter cluster topologies.

---

## Compatibility Commitment

All v0.x releases maintain binary wire compatibility with **Apache Pekko 1.x** and
**Lightbend Akka 2.6+** for the Artery TCP transport layer.

> **MANDATORY RULE (all future features):** Any network-related feature MUST include
> binary compatibility tests with Scala (Pekko/Akka) from the initial implementation phase.
> No network feature may be merged without a passing `//go:build integration` test that
> exercises the wire format against a live Scala node.

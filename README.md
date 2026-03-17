# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.8.0--dev-orange)](https://github.com/sopranoworks/gekka) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

`gekka` is a distributed actor model library for Go, engineered for seamless interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka).

Powered by its own high-performance HOCON engine, [`gekka-config`](https://github.com/sopranoworks/gekka-config), `gekka` supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.

---

## Operational Excellence

Two standalone binaries ship with the gekka module for cluster operators.

### gekka-cli

`gekka-cli` is a command-line interface for inspecting and managing a live cluster via the HTTP Management API.

```
gekka-cli [--config FILE] [--profile NAME] [--json] <subcommand>
```

| Subcommand | Description |
|---|---|
| `members` | List all cluster members with status, roles, DC, and reachability |
| `leave` | Initiate a graceful leave for a named member (`PUT /cluster/members/{address}`) |
| `down` | Mark a member as Down immediately (`DELETE /cluster/members/{address}`) |

**Config file** (`~/.gekka/config.yaml` by default):

```yaml
management_url: http://127.0.0.1:8558
default_profile: local

profiles:
  local:
    management_url: http://127.0.0.1:8558
  staging:
    management_url: http://staging-node:8558
```

The `--url` flag always overrides the config file.  The `--profile` flag selects a named profile.

### gekka-metrics

`gekka-metrics` is a **full cluster node** that joins the cluster with the `metrics-exporter` role and reads gossip state directly — no HTTP polling, no dependency on the Management API.  It exports cluster-state metrics via **OpenTelemetry Protocol (OTLP/HTTP)**.

```
gekka-metrics --config FILE [--otlp ENDPOINT]
```

**HOCON configuration** (same format as any cluster node):

```hocon
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2560 }
  cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
}

gekka.telemetry.exporter.otlp {
  endpoint = "http://otel-collector:4318"
}
```

The `metrics-exporter` role is injected automatically so sharding allocators and singleton managers exclude this node from hosting production workloads.

**Exported metric:**

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `gekka.cluster.members` | ObservableGauge | `status`, `dc` | Member count per status/DC combination |

When no OTLP endpoint is configured the process still joins and emits structured JSON log lines (`cluster_state`) every 30 s.

---

## Current Focus: Rolling Update Reliability

The v0.8.0-dev cycle targets zero-downtime rolling updates for Kubernetes-hosted clusters.  Two mechanisms work together:

### /health/ready drain gate

When `GracefulShutdown` is called, the **first** coordinated-shutdown task (`service-unbind`) calls `ManagementServer.SetShuttingDown()`.  From that point every `GET /health/ready` request returns **503 Service Unavailable** with body:

```json
{"status":"not_ready","reason":"shutting_down"}
```

This causes Kubernetes to stop routing traffic to the node immediately — before any cluster membership changes occur — giving in-flight requests time to complete while the node is still fully joined.

### Shard handoff handshake

Before `cluster-leave` runs, each `ShardRegion` actor executes `PostStop`:

1. Sends `RegionHandoffRequest{RegionPath}` to the `ShardCoordinator`.
2. Blocks on an internal channel (10-second timeout) waiting for `HandoffComplete`.
3. The coordinator releases all locally-owned shards from its allocation table and replies `HandoffComplete{RegionPath}`.
4. The log line **"Handoff Completed"** is emitted; `PostStop` returns; the node proceeds to `cluster-leave`.

This guarantees the coordinator can reallocate shards to surviving members before the departing node's membership status changes, preventing missed-message windows during pod restarts.

**Rolling-update shutdown sequence:**

```
service-unbind              → /health/ready returns 503 "shutting_down"
cluster-sharding-shutdown   → RegionHandoffRequest → HandoffComplete (10 s window)
cluster-leave               → Leave → Exiting → Removed
cluster-shutdown            → CRDT replicator stop
actor-system-terminate      → TCP close
```

---

## Verified Interoperability

`gekka` is verified against live JVM nodes for both **Apache Pekko 1.0.x** and **Lightbend Akka 2.6.21** using E2E integration tests. This ensures byte-level compatibility for cluster membership, remote messaging (including **Artery TLS** secure transport), distributed state, and **Cluster Singleton** failover.

## New in v0.7.0 (Latest Milestones)

The v0.7.0 cycle introduces mission-critical features for large-scale, resilient distributed systems:

- **Split Brain Resolver (SBR)** — Automated cluster partition resolution using Keep Majority, Keep Oldest, Keep Referee, and Static Quorum strategies.
- **Multi-Data Center (Multi-DC) Support** — Data center awareness via `dc-` role convention, enabling DC-specific Cluster Singletons and Sharding affinity.
- **Advanced Cluster Sharding** — Entity Passivation for memory optimization and Remember Entities via event sourcing for automatic state recovery.
- **SQL Persistence Backend** — A pluggable, driver-agnostic SQL backend for journaling and snapshotting, fully verified with PostgreSQL.

## Key Features

- **Hierarchical Actor System** — Parent-child relationships with reliable lifecycle management.
- **Self-Healing Supervision** — Automatic fault tolerance with `OneForOneStrategy`.
- **Pekko/Akka Compatibility** — Verified interop with Scala/Java actors via Artery TCP (Pekko 1.0.x / Akka 2.6.21).
- **Split Brain Resolver** — Hardened cluster integrity during network partitions.
- **Multi-DC Awareness** — Optimized routing and management across geographical regions.
- **Advanced Sharding** — Location-transparent actor placement with passivation and durable recovery.
- **SQL Persistence** — Driver-agnostic event sourcing and snapshotting (PostgreSQL verified).
- **Secure Communication (TLS)** — Binary-compatible Artery TLS support using Go's `crypto/tls`.
- **Cluster Singletons** — Automatic failover and lifecycle management across mixed Go/JVM clusters.
- **Reliable Delivery** — At-least-once delivery (Serializer ID 36) between Go and Scala/Pekko.
- **Type-safe Actors using Go Generics** — Compile-time safety for message passing.
- **Actor Persistence & Event Sourcing** — State recovery via event journaling and snapshotting.
- **Distributed Pub/Sub (Pekko Compatible)** — Decentralized messaging with GZIP-compressed gossip state (Serializer ID 9).
- **Distributed Data / CRDTs** — Decentralized state replication (G-Counter, OR-Set) with Serializer ID 11/12.
- **Location Transparency** — Identical `Tell` and `Ask` semantics for local and remote actors.
- **Extensible Serialization** — Built-in support for Protobuf (ID 2), Raw Bytes (ID 4), and JSON (ID 9).
- **Coordinated Shutdown** — Pekko-compatible phased exit with readiness drain gate, shard handoff handshake, and CRDT flush.

---

## Quick Start: Classic Actor

The simplest entry point — a local actor with no cluster or networking required.

```go
package main

import (
	"log"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

type HelloActor struct {
	actor.BaseActor
}

func (a *HelloActor) Receive(msg any) {
	if s, ok := msg.(string); ok {
		log.Printf("Received: %s", s)
	}
}

func main() {
	system, _ := gekka.NewActorSystem("HelloSystem")

	ref, _ := system.ActorOf(gekka.Props{
		New: func() actor.Actor {
			return &HelloActor{BaseActor: actor.NewBaseActor()}
		},
	}, "hello")

	ref.Tell("Hello, world!")
}
```

---

## Quick Start: Joining a Cluster

Initialize your node to join an existing Pekko/Akka cluster. `gekka` handles Artery handshakes and membership synchronization automatically.

```go
package main

import (
	"log"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

func main() {
	// 1. Initialize the cluster and join as a member
	cluster, err := gekka.NewCluster(gekka.ClusterConfig{
		SystemName: "MyCluster",
		Port:       2553,
		// Provide seed nodes to join an existing cluster
		SeedNodes: []actor.Address{
			{Protocol: "pekko", System: "MyCluster", Host: "192.168.1.10", Port: 2552},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Shutdown()

	log.Printf("Gekka cluster started at %s, joining cluster...", cluster.Addr())
}
```

## Quick Start: Typed Actors (Go Generics)

Gekka provides a **Typed Actor API** leveraging Go Generics for compile-time type safety.

`gekka.Spawn` is the Go-idiomatic equivalent of Pekko/Akka's `system.spawn(behavior, name)`.
Because Go does not permit generic methods on interfaces, the `ActorSystem` is passed as the
first argument — the semantics are identical.

```go
package main

import (
	"fmt"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

type Greet struct{ Name string }

func Greeter() actor.Behavior[Greet] {
	return func(ctx actor.TypedContext[Greet], msg Greet) actor.Behavior[Greet] {
		fmt.Printf("Hello, %s!\n", msg.Name)
		return actor.Same[Greet]()
	}
}

func main() {
	system, _ := gekka.NewActorSystem("TypedSystem")

	// Spawn a typed actor — equivalent to system.spawn(Greeter(), "greeter") in Pekko
	ref, _ := gekka.Spawn(system, Greeter(), "greeter")

	// Send a type-safe message
	ref.Tell(Greet{Name: "Gopher"})
}
```

More examples — local actors, pub/sub, CRDTs, persistence, singletons, coordinated shutdown, and reliable delivery — are in [docs/EXAMPLES.md](docs/EXAMPLES.md).

---

## Artery TLS

`gekka` supports secure transport via Artery TLS, maintaining binary compatibility with Pekko/Akka's `tls-tcp` transport. While JVM nodes typically use JKS keystores, `gekka` leverages Go's `crypto/tls` to provide a modern, PEM-based alternative for managing certificates and private keys.

### HOCON Configuration

Enable TLS by setting the `transport` to `tls-tcp` and providing the paths to your PEM files:

```hocon
pekko.remote.artery {
  transport = "tls-tcp"
  tls {
    certificate = "/path/to/cert.pem"
    private-key = "/path/to/key.pem"
    ca-certificates = "/path/to/ca.pem"
    # Optional: require-client-auth, server-name, min-version
  }
}
```

Support for mutual TLS (mTLS) is built-in, ensuring that only authenticated nodes can join the cluster.

---

## New in v0.6.0 (Stable)

Gekka v0.6.0 is a major stable release focused on **Advanced Clustering** and **Enterprise-Grade Remoting**. Key additions include:

- **Distributed Pub/Sub** — Full Pekko compatibility with GZIP compression support (Serializer ID 9).
- **Artery TLS Transport** — Secure, encrypted cluster communication using PEM-based certificates.
- **Reliable Delivery** — At-least-once messaging protocol for guaranteed delivery matching Pekko's Serializer ID 36.
- **Cluster Singleton** — Automatic lifecycle management and failover of singletons across the cluster.
- **Coordinated Shutdown** — Graceful, phased node exit sequence ensuring clean shard handovers and state flushes.
- **Distributed Data (CRDTs)** — Optimized G-Counter and OR-Set replication for decentralized state.
- **Verified Interoperability** — Comprehensive E2E testing against Apache Pekko 1.0.x and Lightbend Akka 2.6.21.

v0.6.0 also includes **Pool** and **Group Routers** that can be configured directly in HOCON:

```hocon
gekka.actor.deployment {
  "/user/workerPool" {
    router = round-robin-pool
    nr-of-instances = 5
  }
}
```

See [ROUTING.md](docs/ROUTING.md) for more details.

---

## Documentation

- [**Examples**](docs/EXAMPLES.md) — Extended code examples for all major features.
- [**API Reference**](docs/API.md) — Detailed function and method signatures.
- [**Protocol Notes**](docs/PROTOCOL.md) — Artery TCP framing, serialization IDs, and CRDTs.
- [**Routing Features**](docs/ROUTING.md) — Pool/Group routers and HOCON deployment.
- [**Cluster Sharding**](docs/SHARDING.md) — Distributed actor placement, passivation, and remember-entities.
- [**Split Brain Resolver**](docs/SBR.md) — Partition resolution strategies and interoperability testing.
- [**Multi-Data Center**](docs/MULTI_DC.md) — DC-aware cluster configuration and routing.
- [**Secure Transport (TLS)**](docs/TLS.md) — Configuring and using Artery TLS with PEM certificates.

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `internal/proto/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.

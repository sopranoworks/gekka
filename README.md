# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.11.0--dev-blue)](https://github.com/sopranoworks/gekka)

 [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

A Go implementation of the Pekko/Akka actor protocol, with wire-level interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka).

Configuration is loaded via [`gekka-config`](https://github.com/sopranoworks/gekka-config), a HOCON engine that supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.

**Requirement**: Go 1.26.1 or later.

---

## Features

- **Hierarchical Actor System** — Parent-child relationships with supervisor-managed lifecycle.
- **Supervision** — Fault isolation with `OneForOneStrategy`.
- **Pekko/Akka Compatibility** — Wire-level interop with Scala/Java actors via Artery TCP (Pekko 1.1.x / Akka 2.6.21).
- **Split Brain Resolver** — Partition resolution during network splits (Keep Majority, Keep Oldest, Static Quorum).
- **Multi-DC Awareness** — Routing and management across multiple data centers.
- **Cluster Sharding** — Location-transparent actor placement with passivation and durable recovery.
- **SQL Persistence** — Driver-agnostic event sourcing and snapshotting (PostgreSQL verified).
- **Artery TLS** — Encrypted cluster transport using Go's `crypto/tls`, binary-compatible with Pekko's `tls-tcp`.
- **Cluster Singletons** — Singleton failover and lifecycle management across mixed Go/JVM clusters.
- **Reliable Delivery** — At-least-once delivery (Serializer ID 36) compatible with Pekko.
- **Typed Actors (Go Generics)** — Compile-time message type safety with **Timers** and **Stash** support.
- **Actor Persistence** — State recovery via event journaling and snapshotting.
- **Gekka Streams** — Full reactive-streams DSL: Source/Flow/Sink, async boundaries, graph operators (Merge, Broadcast, Balance, Zip, GroupBy), resilience (Restart, Recover), and File IO. See [docs/STREAMS.md](docs/STREAMS.md).
- **Distributed Streams (StreamRefs)** — Share a `Source` or `Sink` across network nodes with end-to-end back-pressure via `TypedSourceRef` / `TypedSinkRef`. TLS-encrypted TCP transport supported.
- **Kubernetes-native Discovery** — Automatic cluster formation via K8s API or DNS SRV.
- **Zero-copy Serialization** — High-performance transport framing with 8.5x faster throughput.
- **Distributed Pub/Sub** — Decentralized messaging with GZIP-compressed gossip (Serializer ID 9).
- **Distributed Data / CRDTs** — G-Counter and OR-Set replication (Serializer ID 11/12).
- **Pool and Group Routers** — Round-robin and random routing, configurable via HOCON deployment config.
- **Location Transparency** — `Tell` and `Ask` work identically for local and remote actors.
- **Extensible Serialization** — Protobuf (ID 2), raw bytes (ID 4), and JSON (ID 9).
- **Coordinated Shutdown** — Phased exit with readiness drain gate, shard handoff, and CRDT flush.
- **Rolling Update Support** — `/health/ready` drain gate and shard handoff handshake for zero-downtime pod restarts.

---

## Verified Interoperability

`gekka` is tested against live JVM nodes for both **Apache Pekko 1.1.x** and **Lightbend Akka 2.6.21** using E2E integration tests, covering cluster membership, remote messaging (including Artery TLS), distributed state, and Cluster Singleton failover.

---

## Operational Tooling

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
| `discovery-check` | Diagnostic tool for testing Kubernetes API/DNS discovery settings |

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

When no OTLP endpoint is configured the process still joins and exports metrics locally.

---

## Rolling Update Support

v0.9.0 continues to support zero-downtime rolling updates in Kubernetes-hosted clusters through coordinated mechanisms.

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

This ensures the coordinator can reallocate shards to surviving members before the departing node's membership status changes, preventing missed-message windows during pod restarts.

**Shutdown sequence:**

```
service-unbind              → /health/ready returns 503 "shutting_down"
cluster-sharding-shutdown   → RegionHandoffRequest → HandoffComplete (10 s window)
cluster-leave               → Leave → Exiting → Removed
cluster-shutdown            → CRDT replicator stop
actor-system-terminate      → TCP close
```

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

Initialize a node to join an existing Pekko/Akka cluster. `gekka` performs the Artery handshake and membership synchronization.

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

The **Typed Actor API** uses Go Generics to enforce message types at compile time.

`gekka.Spawn` is the versatile entry point for spawning actors. It accepts both an `ActorSystem`
and a `TypedContext` (ActorContext), making it easy to build dynamic actor hierarchies.

```go
package main

import (
	"fmt"

	"github.com/sopranoworks/gekka"
)

type Greet struct{ Name string }

func Greeter() gekka.Behavior[Greet] {
	return func(ctx gekka.TypedContext[Greet], msg Greet) gekka.Behavior[Greet] {
		fmt.Printf("Hello, %s!\n", msg.Name)
		
		// 3. Spawn a child actor using the same Spawn function (passing 'ctx' as spawner)
		// child, _ := gekka.Spawn(ctx, ChildBehavior(), "child")
		
		return gekka.Same[Greet]()
	}
}

func main() {
	// 1. Initialize system with a root behavior
	system, _ := gekka.NewActorSystemWithBehavior(Greeter(), "root")

	// 2. Or spawn a top-level actor from an existing system
	ref, _ := gekka.Spawn(system, Greeter(), "greeter")

	// Send a type-safe message
	ref.Tell(Greet{Name: "Gopher"})
}
```

More examples — local actors, reactive streams, pub/sub, CRDTs, persistence, singletons, coordinated shutdown, and reliable delivery — are in [docs/EXAMPLES.md](docs/EXAMPLES.md).

---

## Artery TLS

`gekka` supports Artery TLS, maintaining binary compatibility with Pekko/Akka's `tls-tcp` transport. Go nodes use PEM-based certificates via `crypto/tls` in place of JVM keystores.

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

Mutual TLS (mTLS) is supported; nodes must present a valid certificate to connect.

---

## Documentation

- [**Examples**](docs/EXAMPLES.md) — Extended code examples for all major features.
- [**Porting Guide**](docs/PORTING_GUIDE.md) — Migration tips and conceptual mapping from Apache Pekko / Lightbend Akka.
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

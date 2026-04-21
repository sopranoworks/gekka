# gekka &nbsp;[![Version](https://img.shields.io/badge/version-1.0.0--rc2-blue)](https://github.com/sopranoworks/gekka)

 [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

A Go implementation of the Pekko/Akka actor protocol, with wire-level interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka).

Configuration is loaded via [`gekka-config`](https://github.com/sopranoworks/gekka-config), a HOCON engine that supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.

**Requirement**: Go 1.26.1 or later.

---

## What's New in v1.0.0-rc2

- **Pekko-compatible Cluster Bootstrap Config** — Discovery config migrated from `gekka.cluster.discovery` to `pekko.management.cluster.bootstrap` for full Pekko HOCON compatibility. Old namespace still works with deprecation warning.
- **Pekko-compatible Cluster Configuration** — Standard `pekko.cluster.*` settings now parsed directly: `failure-detector` (threshold, heartbeat-interval, acceptable-heartbeat-pause), `min-nr-of-members`, `retry-unsuccessful-join-after`, `gossip-interval`. Existing Pekko/Akka configs work without translation.
- **gekka-dashboard** — New web-based operational console ([gekka-dashboard](https://github.com/sopranoworks/gekka-dashboard)) with real-time WebSocket cluster monitoring, dark/light theme, and management actions.
- **Notification Engine** — Event-driven alert dispatcher in [gekka-metrics](https://github.com/sopranoworks/gekka-metrics) with Email/Slack channels, role-based filtering, and per-rule throttle.

## Configuration

Gekka uses HOCON for flexible, layered configuration. Standard `pekko.*` and `akka.*` configuration keys are supported directly for full compatibility with existing Pekko/Akka configs.

### Cluster Settings (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.min-nr-of-members` | `1` | Min members before leader promotes Joining to Up |
| `pekko.cluster.retry-unsuccessful-join-after` | `10s` | InitJoin retry interval |
| `pekko.cluster.gossip-interval` | `1s` | Duration between gossip rounds |
| `pekko.cluster.failure-detector.threshold` | `8.0` | Phi threshold for unreachable declaration |
| `pekko.cluster.failure-detector.heartbeat-interval` | `1s` | Heartbeat send interval |
| `pekko.cluster.failure-detector.acceptable-heartbeat-pause` | `3s` | Tolerable heartbeat gap |
| `pekko.cluster.failure-detector.max-sample-size` | `1000` | Heartbeat history window |
| `pekko.cluster.failure-detector.monitored-by-nr-of-members` | `9` | Max heartbeat targets per node |
| `pekko.cluster.leader-actions-interval` | `1s` | Independent leader action ticker |
| `pekko.cluster.periodic-tasks-initial-delay` | `1s` | Delay before first periodic tick |
| `pekko.cluster.shutdown-after-unsuccessful-join-seed-nodes` | `off` | Shutdown on join timeout (`off` = disabled) |
| `pekko.cluster.allow-weakly-up-members` | `7s` | WeaklyUp promotion timeout (`off` = disabled) |
| `pekko.cluster.log-info` | `on` | Gate info-level cluster log messages |
| `pekko.cluster.log-info-verbose` | `off` | Verbose heartbeat, phi, and gossip logging |
| `pekko.cluster.app-version` | `"0.0.0"` | Application version advertised in handshake |
| `pekko.cluster.gossip-different-view-probability` | `0.8` | Prefer different-view gossip targets |
| `pekko.cluster.reduce-gossip-different-view-probability` | `400` | Halve probability at large cluster sizes |
| `pekko.cluster.gossip-time-to-live` | `2s` | Discard stale incoming gossip |
| `pekko.cluster.unreachable-nodes-reaper-interval` | `1s` | Periodic phi re-evaluation interval |
| `pekko.cluster.prune-gossip-tombstones-after` | `24h` | Prune removed-member tombstones |
| `pekko.cluster.down-removal-margin` | `off` | Delay Down→Removed transition |
| `pekko.cluster.seed-node-timeout` | `5s` | Warn when seed doesn't respond in time |
| `pekko.cluster.quarantine-removed-node-after` | `5s` | Schedule UID quarantine after removal |
| `pekko.cluster.run-coordinated-shutdown-when-down` | `on` | Trigger shutdown when self is downed |
| `pekko.cluster.configuration-compatibility-check.enforce-on-join` | `on` | Validate config on incoming InitJoin |
| `pekko.cluster.role.{name}.min-nr-of-members` | — | Per-role gating before leader promotes |

### Singleton & Singleton Proxy (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.singleton.role` | `""` | Restrict singleton to nodes with this role |
| `pekko.cluster.singleton.hand-over-retry-interval` | `1s` | Retry interval during leadership transfer |
| `pekko.cluster.singleton-proxy.singleton-identification-interval` | `1s` | Periodic re-resolution of singleton location |
| `pekko.cluster.singleton-proxy.buffer-size` | `1000` | Buffer messages while singleton is unknown |

### Sharding (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.sharding.number-of-shards` | `1000` | Total shard count (immutable after start) |
| `pekko.cluster.sharding.role` | `""` | Restrict sharding to nodes with this role |
| `pekko.cluster.sharding.remember-entities` | `off` | Persist entity lifecycle for auto-respawn |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | `120s` | Stop idle entities after this duration |

### Split Brain Resolver (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.split-brain-resolver.active-strategy` | `""` | Strategy: keep-majority, keep-oldest, static-quorum, keep-referee |
| `pekko.cluster.split-brain-resolver.stable-after` | `20s` | Stability window before decision |
| `pekko.cluster.split-brain-resolver.down-all-when-unstable` | `on` | Down all nodes if instability persists |

### Pub-Sub (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.cluster.pub-sub.gossip-interval` | `1s` | Subscription gossip interval |

### Persistence

| Key | Default | Description |
|---|---|---|
| `pekko.persistence.max-concurrent-recoveries` | `50` | Global semaphore for concurrent actor recoveries |

### Remote Transport (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.remote.artery.advanced.maximum-frame-size` | `256 KiB` | Maximum Artery frame payload size |
| `pekko.remote.artery.bind.hostname` | `""` | NAT/Docker: bind address (advertise canonical) |
| `pekko.remote.artery.bind.port` | `""` | NAT/Docker: bind port (advertise canonical) |

### Management (Pekko Management-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.management.http.port` | `8558` | TCP port for the HTTP Management API |
| `pekko.management.http.hostname` | `127.0.0.1` | Binding interface for the Management API |
| `pekko.management.http.enabled` | `false` | Explicitly enable/disable the Management API |

### Other Settings (Pekko-compatible)

| Key | Default | Description |
|---|---|---|
| `pekko.loglevel` | `INFO` | Minimum log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `pekko.cluster.distributed-data.enabled` | `false` | Enable the CRDT Distributed Data replicator |
| `pekko.cluster.distributed-data.gossip-interval` | `2s` | Interval between DData gossip rounds |
| `gekka.telemetry.exporter.otlp.endpoint` | `""` | OTLP/HTTP collector endpoint for metrics/traces |

### Auto-Enable Logic
If either `pekko.management.http.hostname` or `pekko.management.http.port` is explicitly defined in your configuration, the Management Server will be enabled automatically (`enabled = true`).

---

## Features

### 🏗️ Core Actor Engine
- **Hierarchical Actor System** — Parent-child relationships with supervisor-managed lifecycle.
- **Typed Behaviors** — Type-safe actor definitions leveraging Go generics for robust messaging.
- **Fault Tolerance** — Advanced supervision with `OneForOneStrategy`, `AllForOneStrategy`, `Behaviors.supervise` (Restart/Stop/Resume/Backoff), and specialized recovery policies.
- **Timers & Stash** — Built-in `TimerScheduler` for scheduled tasks and `StashBuffer` for message deferral.
- **EventStream & DeadLetter** — System-wide pub/sub bus for broadcasting events; undeliverable messages published as `DeadLetter` events.
- **Custom Mailboxes** — Pluggable `MailboxFactory` interface with `BoundedMailbox` (drop strategies), `UnboundedPriorityMailbox` (heap-backed priority ordering), and `LoggingMailbox` decorator.
- **Dispatchers** — `PinnedDispatcher` (OS-thread affinity), `CallingThreadDispatcher` (synchronous test execution), HOCON-configurable dispatcher assignment via `Props.DispatcherKey`.
- **Classic Actor Lifecycle** — `become`/`unbecome` behavior stack; `PoisonPill`/`Kill`; `Identify`/`ActorIdentity`; `GracefulStop`; `SetReceiveTimeout`/`CancelReceiveTimeout`.
- **Typed Actor Interaction** — `PipeToSelf`, `MessageAdapter`, `AskWithStatus`, `WatchWith`, `SetReceiveTimeout` (typed); `SpawnProtocol`; `TransformMessages`; `ReceiveSignal`; `ReceivePartial`; `WithMdc`.
- **Extension Framework** — `ExtensionId`/`Extension` lazy singleton API for attaching system-level services.

### 🌐 Clustering & Distribution
- **Artery TCP & Aeron UDP Transport** — High-performance, Pekko/Akka-compatible wire protocols. TCP transport with full preamble and manifest support; native Aeron UDP transport for low-latency, lock-free messaging across hybrid Go/JVM clusters.
- **ClusterBootstrap** — Automatic seed-node discovery with configurable quorum logic and leader-election by address sort; eliminates static seed-node configuration.
- **Cluster Sharding** — Automated, load-aware actor placement with manual rebalancing support via CLI.
- **Cluster Client** — External client extension for communicating with a cluster without joining as a member.
- **Multi-source Discovery** — Kubernetes API, DNS SRV, Consul, AWS EC2 tag-based, and `AggregateProvider` (multi-source fan-out with deduplication).
- **Split Brain Resolver (SBR)** — Resilient partition resolution with configurable strategies (static-quorum, keep-oldest, keep-majority, lease-majority, down-all).
- **Multi-DC Awareness** — Strategic routing and management across multiple logical data centers with configurable cross-DC gossip probability.
- **Rolling Updates** — `AppVersion` field in handshake; `AppVersionChanged` events enable blue/green and phased rollout orchestration.

### 💾 Persistence & Reliability
- **Event Sourcing** — Durable state recovery via journaled events and periodic snapshots; `snapshotWhen` predicate; `RetentionCriteria` for automated snapshot scheduling and journal cleanup.
- **Recovery Control** — `RecoveryStrategy` (Disabled/SnapshotSelection/ReplayFilter); `RecoveryCompleted` signal for post-recovery initialization.
- **Replicated Event Sourcing** — Multi-DC persistence with `ReplicatedEventSourcing`; events carry `ReplicaId` metadata and stream across replicas for eventual consistency.
- **Persistent FSM** — State-machine-based persistence combining FSM behavior with the event sourcing journal; `persistAsync`/`persistAllAsync` for high-throughput writes.
- **Event Adapters** — Transformation hooks for schema evolution between journal and actor domain models.
- **Extensible Backends** — Decoupled storage interfaces supporting Spanner, SQL, and Redis via extensions.
- **Exactly-once Reliable Delivery** — Guaranteed message delivery even during shard handoffs or failovers; WorkPulling pattern, `DurableProducerQueue`, and `AtLeastOnceDelivery` mixin.
- **Distributed Data (CRDTs)** — Eventually consistent shared state with bandwidth-efficient delta-propagation; `GCounter`, `ORSet`, `PNCounterMap`, `ORMultiMap`, `GSet` typed APIs.

### 🔌 Ecosystem & Connectivity
- **Pekko/Akka Interoperability** — Verified wire-level compatibility with JVM nodes via Artery TCP and native Aeron UDP. Hybrid clusters (Go + Scala) tested with `sbt multi-jvm:test` including 4-node (2 Akka + 2 Go) cluster formation.
- **HOCON Configuration** — Flexible, layered configuration powered by the `gekka-config` engine, including user-defined serializer registration and dispatcher configuration.
- **Gekka Streams** — Backpressure-aware reactive streams aligned with the Akka Streams model; rich operator set including hubs, compression, framing, sub-streams, custom `GraphStage` API, and Reactive Streams interop.
- **Remote Actor Deployment** — `RemoteScope`/`RemoteDeploy` for spawning actors on remote nodes via `ActorOf`.
- **Serialization** — Built-in Protobuf, ByteArray, JSON, CBOR; HOCON-configurable user-defined serializers.
- **Testing Framework** — `BehaviorTestKit`, `SerializationTestKit`, `ManualTime`, `PersistenceTestKit`, `LoggingTestKit`, `MultiNodeTestKit`, `TestProbe`, stream probes.

### 📊 Observability & Management
- **Interactive Dashboard** — Real-time TUI dashboard in `gekka-cli` for monitoring member health and roles.
- **Management API** — Comprehensive HTTP endpoints for cluster introspection and shard rebalancing.
- **Distributed Tracing** — OpenTelemetry integration for end-to-end visibility across the actor pipeline.
- **Real-time Metrics** — `gekka-metrics` tool for exporting cluster state to OTel collectors.

---

## Performance & Scalability

Gekka is engineered for mission-critical performance. Our latest benchmarks verify:

- **Sub-second Recovery**: 1,000 persistent actors can recover from cold storage in under 500ms using the Spanner Native backend.
- **High Throughput**: Zero-copy serialization and optimized Artery framing support millions of messages per second.
- **Round-trip Latency**: ~20,600 remote Ask round-trips/s over Artery TCP with < 1% tracing overhead.

To run these benchmarks yourself:
```bash
go test -v -bench=BenchmarkRecovery ./test/bench/  # Requires Spanner Emulator or Postgres
```

For more detailed results, see the [Benchmark Report](docs/BENCHMARKS.md).

---

## Verified Interoperability

`gekka` is tested against live JVM nodes for both **Apache Pekko 1.1.x** and **Lightbend Akka 2.6.21** using E2E integration tests, covering cluster membership, remote messaging (including Artery TLS), distributed state, and Cluster Singleton failover.

---

## Operational Tooling

Gekka provides two companion CLI tools, maintained as independent repositories:

- **[gekka-cli](https://github.com/sopranoworks/gekka-cli)** -- interactive cluster management, shard rebalancing, TUI dashboard
- **[gekka-metrics](https://github.com/sopranoworks/gekka-metrics)** -- real-time cluster metrics via OpenTelemetry (joins the ring as a gossip-aware exporter)

For metrics reference and tracing configuration, see the [Operational Tooling Guide](docs/OPERATIONAL_TOOLING.md).

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

### Quick Start: Streaming API

Build type-safe processing pipelines with back-pressure.

```go
package main

import (
	"fmt"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/stream"
)

func main() {
	system, _ := gekka.NewActorSystem("StreamSystem")

	// 1. Define stages
	source := stream.FromSlice([]int{1, 2, 3})
	flow := stream.Map(func(i int) int { return i * 2 })
	sink := stream.Foreach(func(i int) { fmt.Println(i) })

	// 2. Connect stages and run with a materializer
	// stream.Via(source, flow).To(sink) creates a RunnableGraph
	graph := stream.Via(source, flow).To(sink)
	
	// 3. Execute the graph
	graph.Run(stream.ActorMaterializer{})
}
```

More examples — local actors, reactive streams, pub/sub, CRDTs, persistence, singletons, coordinated shutdown, and reliable delivery — are in [docs/EXAMPLES.md](docs/EXAMPLES.md).

---

## Aeron UDP Transport

`gekka` supports the native Aeron UDP transport, providing wire-level compatibility with Akka 2.6.x and Pekko 1.x `aeron-udp` nodes. The Go implementation speaks the Aeron 1.30.0 framing protocol directly — no JVM Media Driver required.

### HOCON Configuration

```hocon
pekko.remote.artery {
  transport = aeron-udp
  canonical {
    hostname = "127.0.0.1"
    port     = 2553
  }
}

pekko.cluster {
  seed-nodes = ["akka://MySystem@127.0.0.1:2551"]
  downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
  split-brain-resolver {
    active-strategy = keep-oldest
    stable-after    = 5s
  }
}
```

### Transport Details

| Stream | Aeron Stream ID | Purpose |
|--------|----------------|---------|
| Control | 1 | Artery handshake, cluster heartbeats, compression advertisements |
| Ordinary | 2 | User-level actor messages |
| Large | 3 | Large messages (fragmented across multiple DATA frames) |

All Aeron frames use a **32-byte header** (DATA frames) with little-endian encoding. Reliability is achieved via NACK-based retransmission: subscribers send NAK frames when they detect sequence gaps, and Status Message (SM) frames to advertise receiver window capacity.

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
- [**Benchmark Report**](docs/BENCHMARKS.md) — Scale, throughput, reliable delivery, tracing, and persistence recovery numbers.
- [**Operational Tooling Guide**](docs/OPERATIONAL_TOOLING.md) — Cluster management via `gekka-cli` and OTel metrics via `gekka-metrics`.

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `internal/proto/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.

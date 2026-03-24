# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.14.0--dev-orange)](https://github.com/sopranoworks/gekka)
L2- 
L3-  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)
L4- 
L5- ⚠️ Warning: Gekka is currently under active development. The public API and internal protocols are subject to change until v1.0.0 is released.
L6- 
L7- A Go implementation of the Pekko/Akka actor protocol, with wire-level interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka).
L8- 
L9- Configuration is loaded via [`gekka-config`](https://github.com/sopranoworks/gekka-config), a HOCON engine that supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.
L10- 
L11- **Requirement**: Go 1.26.1 or later.
L12- 
L13- ---
L14- 
L15: ## What's New in v0.14.0-dev

- **Artery TCP Wire Compatibility** — Full protocol alignment with Akka 2.6.x and Pekko 1.x, including preamble detection and manifest-based message routing.
- **Auto-enable Management** — The Management Server now automatically enables itself if a `hostname` or `port` is defined in the configuration, simplifying bootstrap.
- **Enhanced TUI Dashboard** — Interactive `gekka-cli dashboard` with dynamic column alignment, a scrolling marquee for long member roles, and standardized exit confirmation (ESC -> Y/n).
- **Granular Logging (slog)** — Integrated structured logging with configurable levels via HOCON (`gekka.logging.level`), significantly reducing default noise while preserving critical protocol traces.
- **Ultra Thin Core** — Strategic extraction of heavy third-party SDKs (Cloud Spanner, OpenTelemetry, Kubernetes) into independent extension sub-modules under `/extensions/`.
- **Plugin-based Persistence** — Standard library-first interfaces for `JournalStore` and `SnapshotStore`, enabling dependency-injected backends without core bloat.

## Configuration

Gekka uses HOCON for flexible, layered configuration. Below are the key Gekka-specific settings:

| Key | Default | Description |
|---|---|---|
| `gekka.logging.level` | `INFO` | Minimum log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `gekka.management.http.port` | `8558` | TCP port for the HTTP Management API |
| `gekka.management.http.hostname` | `127.0.0.1` | Binding interface for the Management API |
| `gekka.telemetry.exporter.otlp.endpoint` | `""` | OTLP/HTTP collector endpoint for metrics/traces |

### Auto-Enable Logic
If either `gekka.management.http.hostname` or `gekka.management.http.port` is explicitly defined in your configuration, the Management Server will be enabled automatically (`enabled = true`).

---

## Features

### 🏗️ Core Actor Engine
- **Hierarchical Actor System** — Parent-child relationships with supervisor-managed lifecycle.
- **Typed Behaviors** — Type-safe actor definitions leveraging Go generics for robust messaging.
- **Fault Tolerance** — Advanced supervision with `OneForOneStrategy` and specialized recovery policies.
- **Timers & Stash** — Built-in `TimerScheduler` for scheduled tasks and `StashBuffer` for message deferral.

### 🌐 Clustering & Distribution
- **Artery TCP Transport** — High-performance, Pekko-compatible wire protocol with full preamble and manifest support.
- **Cluster Sharding** — Automated, load-aware actor placement with manual rebalancing support via CLI.
- **Kubernetes-native Discovery** — Automated cluster formation using the Kubernetes API or DNS SRV.
- **Split Brain Resolver (SBR)** — Resilient partition resolution with configurable strategies (static-quorum, keep-oldest).
- **Multi-DC Awareness** — Strategic routing and management across multiple logical data centers.

### 💾 Persistence & Reliability
- **Event Sourcing** — Durable state recovery via journaled events and periodic snapshots.
- **Extensible Backends** — Decoupled storage interfaces supporting Spanner, SQL, and Redis via extensions.
- **Exactly-once Reliable Delivery** — Guaranteed message delivery even during shard handoffs or failovers.
- **Distributed Data (CRDTs)** — Eventually consistent shared state with bandwidth-efficient Delta-propagation.

### 🔌 Ecosystem & Connectivity
- **Pekko/Akka Interoperability** — Verified wire-level compatibility with JVM nodes via standard Artery TCP.
- **HOCON Configuration** — Flexible, layered configuration powered by the `gekka-config` engine.
- **Gekka Streams** — Backpressure-aware reactive streams aligned with the Akka Streams model.

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

Gekka provides a powerful suite of operational tools for managing and monitoring your cluster.
Use **`gekka-cli`** for dynamic shard rebalancing and cluster management, and **`gekka-metrics`** for real-time observability via OpenTelemetry.

For detailed usage and command references, see the [Operational Tooling Guide](docs/OPERATIONAL_TOOLING.md).

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

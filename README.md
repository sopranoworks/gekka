# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.6.0--dev-orange)](https://github.com/sopranoworks/gekka) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

**Pekko/Akka Dual-Compatibility**

`gekka` is a distributed actor model library for Go, engineered for seamless interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka) via the Artery TCP protocol. It provides a robust **Hierarchical Actor System**, **Self-Healing Supervision**, and true **Location Transparency**.

Powered by its own high-performance HOCON engine, [`gekka-config`](https://github.com/sopranoworks/gekka-config), `gekka` supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.

## Verified Interoperability

`gekka` is verified against live JVM nodes for both **Apache Pekko 1.0.x** and **Lightbend Akka 2.6.21** using E2E integration tests. This ensures byte-level compatibility for cluster membership, remote messaging (including **Artery TLS** secure transport), and distributed state.


## Key Features

- **Hierarchical Actor System** — Parent-child relationships with reliable lifecycle management.
- **Self-Healing Supervision** — Automatic fault tolerance with `OneForOneStrategy` (Restart, Resume, Stop, Escalate).
- **Pekko/Akka Remote & Cluster Compatibility** — Verified interop with Scala/Java actors via Artery TCP.
- **Secure Communication (TLS)** — Binary-compatible Artery TLS support for encrypted cluster traffic using Go's `crypto/tls`.
- **Type-safe Actors using Go Generics** — Compile-time safety for message passing.
- **Actor Persistence & Event Sourcing** — State recovery via event journaling and snapshotting.
- **Distributed Pub/Sub (Pekko Compatible)** — Decentralized messaging with GZIP-compressed gossip state (Serializer ID 9).
- **Distributed Data / CRDTs** — Decentrallized state replication (G-Counter, OR-Set) with Serializer ID 11/12.
- **Location Transparency** — Identical `Tell` and `Ask` semantics for local and remote actors.
- **Location Transparent Senders** — Reply to originators without manual address tracking.
- **Extensible Serialization** — Built-in support for Protobuf (ID 2), Raw Bytes (ID 4), and JSON (ID 9).
- **Actor-aware Logging** — Structured logging contextualized with actor paths and system info.
- **High-Performance Remoting** — Binary-compatible Artery TCP with transport-level heartbeats.
- **Observability** — Built-in monitoring with `/healthz` and `/metrics` (JSON/Prometheus).
- **Cluster Singletons** — Distributed singleton management with auto-failover.

## Quick Start 1: Local Actor System

For applications that don't need networking, `gekka` provides a lightweight `LocalActorSystem`. This is ideal for in-process concurrency with actor semantics.

```go
package main

import (
	"log"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

func main() {
	// 1. Initialize a local-only actor system (no networking)
	system, err := gekka.NewActorSystem("LocalSystem")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Create an actor
	ref, _ := system.ActorOf(gekka.Props{
		New: func() actor.Actor { return &MyActor{BaseActor: actor.NewBaseActor()} },
	}, "worker")

	// 3. Send a message
	ref.Tell("Hello Local!")
}

type MyActor struct { actor.BaseActor }
func (a *MyActor) Receive(msg any) { log.Printf("Got: %v", msg) }
```

## Quick Start 2: Joining a Cluster

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

## Quick Start 3: Defining and Using Actors

Once joined, you can define actors to handle business logic. This example shows the request-response (**Ask**) pattern.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

// 1. Define your business logic in an actor
type EchoActor struct {
	actor.BaseActor
}

func (a *EchoActor) Receive(msg any) {
	if s, ok := msg.(string); ok {
		a.Log().Info("Received", "payload", s)
		a.Sender().Tell("Echo: "+s, a.Self())
	}
}

func main() {
	cluster, _ := gekka.NewCluster(gekka.ClusterConfig{SystemName: "ExampleSystem"})
	defer cluster.Shutdown()

	// 2. Create a named actor instance
	ref, _ := cluster.System.ActorOf(gekka.Props{
		New: func() actor.Actor { return &EchoActor{BaseActor: actor.NewBaseActor()} },
	}, "echo")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 3. Interact via the Ask pattern (request-response)
	reply, _ := ref.Ask(ctx, "Hello Gekka!")
	log.Printf("Reply: %v", reply)
}
```

## Quick Start 4: Typed Actors (Go Generics)

Gekka provides a **Typed Actor API** leveraging Go Generics for compile-time type safety.

```go
package main

import (
	"fmt"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
)

type Greet struct { Name string }

func Greeter() actor.Behavior[Greet] {
	return func(ctx actor.TypedContext[Greet], msg Greet) actor.Behavior[Greet] {
		fmt.Printf("Hello, %s!\n", msg.Name)
		return actor.Same[Greet]()
	}
}

func main() {
	system, _ := gekka.NewActorSystem("TypedSystem")
	
	// Spawn a typed actor
	ref, _ := gekka.SpawnTyped(system, Greeter(), "greeter")
	
	// Send a type-safe message
	ref.Tell(Greet{Name: "Gopher"})
}
```

## Quick Start 5: Distributed Pub/Sub

`gekka` supports distributed publish-subscribe across the cluster, fully compatible with Pekko's `DistributedPubSub`.

```go
package main

import (
	"context"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/cluster/pubsub"
)

func main() {
	cluster, _ := gekka.NewCluster(...)
	
	// 1. Get the mediator (distributed pub-sub interface)
	mediator := cluster.Mediator()
	
	// 2. Subscribe an actor to a topic
	mediator.Subscribe(context.Background(), "news", "", "/user/my-actor")
	
	// 3. Publish a message to all subscribers cluster-wide
	mediator.Publish(context.Background(), "news", []byte("Hello Cluster!"))
}
```

## Quick Start 6: Distributed Data (CRDTs)

Replicate state across nodes using conflict-free replicated data types.

```go
package main

import (
	"github.com/sopranoworks/gekka"
)

func main() {
	cluster, _ := gekka.NewCluster(...)
	repl := cluster.Replicator()

	// Increment a distributed counter
	repl.IncrementCounter("hits", 1, gekka.WriteLocal)
	
	// Read the merged value from all nodes
	val := repl.GCounter("hits").Value() 
}
```

## Quick Start 7: Persistent Actors (Event Sourcing)

Persistent actors automatically recover their state by replaying events from a journal upon restart.

```go
package main

import (
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

func Counter(id string, journal persistence.Journal) *gekka.EventSourcedBehavior[any, int, int] {
	return &gekka.EventSourcedBehavior[any, int, int]{
		PersistenceID: id,
		Journal:       journal,
		InitialState:  0,
		CommandHandler: func(ctx actor.TypedContext[any], state int, cmd any) actor.Effect[int, int] {
			return actor.Persist[int, int](1)
		},
		EventHandler: func(state int, event int) int {
			return state + event
		},
	}
}
func main() {
	system, _ := gekka.NewActorSystem("PersistenceSystem")
	journal := persistence.NewInMemoryJournal()
	
	// Spawn a persistent actor (automatically recovers state from journal)
	ref, _ := gekka.SpawnPersistent(system, Counter("my-id", journal), "counter")
	ref.Tell(Increment{})
}
```

See the [persistence example](examples/persistence/main.go) for a full implementation including snapshots and recovery demonstration.


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

### How it works

- **Location Transparency**: Messaging works the same way whether the actor is local or remote. The `ActorRef` abstracts away the network layer.
- **HOCON-ready**: Configuration can be passed programmatically via `ClusterConfig` or loaded directly from standard `application.conf` files.


## New in v0.6.0: Distributed Pub/Sub & CRDTs

v0.6.0 introduces **Distributed Pub/Sub** with GZIP compression support and **Distributed Data** (CRDTs) for decentralized state management. This release also features:
- **Artery TLS Transport** — Secure, encrypted cluster communication using PEM-based certificates.
- **Verified Interoperability** — Extensive E2E test suite against Scala Pekko/Akka processes, now including secure transport.
- **Protocol-Aware Configuration** — Automatic switching of configuration keys based on the detected protocol.
- **GZIP Support** — Optimized bandwidth for pub-sub and CRDT gossip.

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

## Documentation

- [**API Reference**](docs/API.md) — Detailed function and method signatures.
- [**Protocol Notes**](docs/PROTOCOL.md) — Artery TCP framing, serialization IDs, and CRDTs.
- [**Routing Features**](docs/ROUTING.md) — Pool/Group routers and HOCON deployment.
- [**Cluster Sharding**](docs/SHARDING.md) — Distributed actor placement and rebalancing.
- [**Secure Transport (TLS)**](docs/TLS.md) — Configuring and using Artery TLS with PEM certificates.

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `internal/proto/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.

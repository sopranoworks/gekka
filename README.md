# gekka &nbsp;[![Version](https://img.shields.io/badge/version-0.4.0--dev-blue)](https://github.com/sopranoworks/gekka) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Go CI](https://github.com/sopranoworks/gekka/actions/workflows/go.yml/badge.svg)](https://github.com/sopranoworks/gekka/actions/workflows/go.yml)

**Pekko/Akka Dual-Compatibility**

`gekka` is a distributed actor model library for Go, engineered for seamless interoperability with [Apache Pekko](https://pekko.apache.org/) and [Lightbend Akka](https://www.lightbend.com/akka) via the Artery TCP protocol. It provides a robust **Hierarchical Actor System**, **Self-Healing Supervision**, and true **Location Transparency**.

Powered by its own high-performance HOCON engine, [`gekka-config`](https://github.com/sopranoworks/gekka-config), `gekka` supports both automatic cluster formation and direct node-to-node communication using the standard `pekko://` and `akka://` URI schemes.

> [!WARNING]
> Development Status: This project is in active development (v0.4.0-dev). We are currently undergoing major refactoring to stabilize the Public API and encapsulate internal logic. Breaking changes are expected until the v1.0.0 release.

## Key Features

- **Hierarchical Actor System** — Parent-child relationships with reliable lifecycle management.
- **Self-Healing Supervision** — Automatic fault tolerance with `OneForOneStrategy` (Restart, Resume, Stop, Escalate).
- **Pekko/Akka Remote & Cluster Compatibility** — Interop with Scala/Java actors via Artery TCP.
- **Location Transparency** — Identical `Tell` and `Ask` semantics for local and remote actors.
- **Location Transparent Senders** — Reply to originators without manual address tracking.
- **Extensible Serialization** — Built-in support for Protobuf (ID 2), Raw Bytes (ID 4), and JSON (ID 9).
- **Actor-aware Logging** — Structured logging contextualized with actor paths and system info.
- **High-Performance Remoting** — Binary-compatible Artery TCP with transport-level heartbeats.
- **Observability** — Built-in monitoring with `/healthz` and `/metrics` (JSON/Prometheus).
- **Cluster Singletons & CRDTs** — Support for Cluster Singletons and Distributed Data (G-Counter, OR-Set).

## Quick Start 1: Joining a Cluster

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
	cluster, err := gekka.Spawn(gekka.ClusterConfig{
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

## Quick Start 2: Defining and Using Actors

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
	cluster, _ := gekka.Spawn(gekka.ClusterConfig{SystemName: "ExampleSystem"})
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

### How it works

- **Location Transparency**: Messaging works the same way whether the actor is local or remote. The `ActorRef` abstracts away the network layer.
- **HOCON-ready**: Configuration can be passed programmatically via `ClusterConfig` or loaded directly from standard `application.conf` files.


## New in v0.4.0: Config-Driven Routing

v0.4.0 introduces **Pool** and **Group Routers** that can be configured directly in HOCON:

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
- [**Integration Tests**](integration_test.go) — End-to-end tests against Scala/Pekko.

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

This library is a Go implementation that interacts with and references the [Apache Pekko](https://pekko.apache.org/) project.

Specific files, such as `internal/proto/cluster/ClusterMessages.pb.go`, are generated from Protobuf definitions provided by the Apache Pekko project and retain their original **Apache License 2.0** and copyright notices from the **Apache Software Foundation** and **Lightbend Inc.** Those notices are reproduced in full within the respective files, as required by the Apache License 2.0.

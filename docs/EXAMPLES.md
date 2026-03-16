# gekka — Code Examples

Extended quick-start examples for every major gekka feature.  The two most
important patterns — joining a cluster and using typed actors — live in the
[main README](../README.md).  Everything else is here.

---

## 1. Local Actor System

For applications that don't need networking, `gekka` provides a lightweight
`LocalActorSystem`.  This is ideal for in-process concurrency with actor
semantics.

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

type MyActor struct{ actor.BaseActor }

func (a *MyActor) Receive(msg any) { log.Printf("Got: %v", msg) }
```

---

## 2. Defining and Using Actors

Once joined, you can define actors to handle business logic.  This example
shows the request-response (**Ask**) pattern.

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

---

## 3. Distributed Pub/Sub

`gekka` supports distributed publish-subscribe across the cluster, fully
compatible with Pekko's `DistributedPubSub`.

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

---

## 4. Distributed Data (CRDTs)

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

---

## 5. Persistent Actors (Event Sourcing)

Persistent actors automatically recover their state by replaying events from a
journal upon restart.

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

See the [persistence example](../examples/persistence/main.go) for a full
implementation including snapshots and recovery demonstration.

---

## 6. Cluster Singletons

Gekka ensures that exactly one instance of a singleton actor is alive in the
cluster, typically on the oldest node.

```go
package main

import (
	"context"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
)

func main() {
	node, _ := gekka.NewCluster(...)

	// 1. Define the singleton manager
	mgr := cluster.NewClusterSingletonManager(node.ClusterManager(), actor.Props{
		New: func() actor.Actor { return &MySingleton{} },
	}, "")

	// 2. Start the manager (which spawns the singleton on the oldest node)
	node.System.ActorOf(gekka.Props{New: func() actor.Actor { return mgr }}, "singletonManager")

	// 3. Access the singleton via a proxy from any node
	proxy := cluster.NewClusterSingletonProxy(
		node.ClusterManager(), node.Router(),
		"/user/singletonManager", "",
	)
	proxy.Send(context.Background(), "Hello Singleton!")
}
```

---

## 7. Coordinated Shutdown (Graceful Exit)

`gekka` implements a Pekko-compatible **Coordinated Shutdown** sequence that
drives the node through the full cluster departure lifecycle —
`Leave → Exiting → Removed` — before closing TCP connections.

```go
package main

import (
	"context"
	"time"

	"github.com/sopranoworks/gekka"
)

func main() {
	node, _ := gekka.NewCluster(gekka.ClusterConfig{...})

	// Register a custom task in any standard phase.
	node.CoordinatedShutdown().AddTask("service-stop", "flush-cache", func(ctx context.Context) error {
		return flushCache(ctx)
	})

	// Trigger graceful cluster exit with a 30-second deadline.
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node.GracefulShutdown(shutCtx)
}
```

Built-in tasks registered automatically:

| Phase | Task | What it does |
|---|---|---|
| `service-unbind` | `mark-management-shutting-down` | Sets `/health/ready` → 503 immediately, draining the load-balancer before any cluster state changes |
| `cluster-sharding-shutdown-region` | `stop-local-regions` | Stops ShardRegions; each sends `RegionHandoffRequest` and waits for `HandoffComplete` from the coordinator |
| `cluster-leave` | `send-leave-and-wait` | Broadcasts Leave; polls gossip until this node reaches Removed |
| `cluster-shutdown` | `stop-replicator` | Stops the CRDT gossip loop |
| `actor-system-terminate` | `close-transport` | Cancels the root context and closes all TCP connections |

Use `node.RegisterShardingRegion(ref)` immediately after spawning a ShardRegion
to enrol it in the sharding shutdown phase.

Typed phase constants (`actor.PhaseServiceUnbind`, `actor.PhaseShardingShutdownRegion`,
etc.) are available in the `actor` package to avoid raw-string phase names.

---

## 8. Reliable Delivery (At-Least-Once)

`gekka` implements Pekko's **Reliable Delivery** protocol (Serializer ID 36)
for guaranteed at-least-once delivery between Go and Scala actors.  The
`actor/delivery` package provides `ProducerController` and `ConsumerController`
typed actors.

```go
package main

import (
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/delivery"
)

// App-level consumer actor that receives Delivery messages.
type MyConsumer struct{ actor.BaseActor }

func (a *MyConsumer) Receive(msg any) {
	if d, ok := msg.(delivery.Delivery); ok {
		// Process the message, then confirm.
		d.ConfirmTo.Tell(delivery.Confirmed{SeqNr: d.SeqNr}, a.Self())
	}
}

func main() {
	node, _ := gekka.NewCluster(gekka.ClusterConfig{...})
	defer node.Shutdown()

	// Register the Reliable Delivery serializer (must be done before any
	// delivery messages are sent or received).
	node.RegisterSerializer(delivery.NewSerializer())

	// ── Go → Scala direction ──────────────────────────────────────────────
	// Spawn a ProducerController targeting Scala's ConsumerController.
	scalaConsumerPath := "pekko://MySystem@scala-host:2552/user/scalaConsumer"
	pc := delivery.NewProducerController("my-producer", scalaConsumerPath, delivery.DefaultWindowSize)
	producerRef, _ := node.System.ActorOf(gekka.Props{New: func() actor.Actor {
		return actor.NewTypedActor[any](pc)
	}}, "goProducer")

	// Enqueue messages — they are delivered reliably, with flow control.
	producerRef.Tell(delivery.SendMessage{
		Payload:      []byte("hello from Go"),
		SerializerID: 4, // ByteArraySerializer
	})

	// ── Scala → Go direction ──────────────────────────────────────────────
	// Spawn a ConsumerController and connect it to Scala's ProducerController.
	appRef, _ := node.System.ActorOf(gekka.Props{New: func() actor.Actor {
		return &MyConsumer{BaseActor: actor.NewBaseActor()}
	}}, "goConsumerApp")

	cc := delivery.NewConsumerController(appRef, delivery.DefaultWindowSize)
	ccRef, _ := node.System.ActorOf(gekka.Props{New: func() actor.Actor {
		return actor.NewTypedActor[any](cc)
	}}, "goConsumer")

	// Initiate registration with the remote ProducerController.
	scalaProducerPath := "pekko://MySystem@scala-host:2552/user/scalaProducer"
	ccRef.Tell(delivery.ConsumerStart{ProducerPath: scalaProducerPath})
}
```

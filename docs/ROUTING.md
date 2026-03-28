# Gekka Routing Features (v0.14.0)

Gekka provides robust config-driven routing, allowing you to distribute messages across multiple actors (routees) using various strategies. Routers were introduced in v0.4.0 and are stable as of v0.13.0.

## Overview

A **Router** is an actor that forwards incoming messages to a set of routees based on a `RoutingLogic`. Gekka supports two main types of routers:

1.  **Pool Router**: Manages its own child actors as routees.
2.  **Group Router**: Routes messages to a pre-existing set of actors (resolved by path).

## Routing Logics

| Logic | Description |
|-------|-------------|
| **Round Robin** | Distributes messages sequentially across routees. |
| **Random** | Selects a routee uniformly at random. |

## Pool Routers

A `PoolRouter` creates and supervises its routees. If a routee stops, the router automatically removes it from the pool.

```go
pool := actor.NewPoolRouter(
    &actor.RoundRobinRoutingLogic{},
    3,
    actor.Props{New: func() actor.Actor {
        return &WorkerActor{BaseActor: actor.NewBaseActor()}
    }},
)
node.System.ActorOf(actor.Props{New: func() actor.Actor { return pool }}, "workerPool")
```

## Group Routers

A `GroupRouter` routes to existing actors. Routees can be supplied as `ActorRef`s or as paths to be resolved.

```go
group := actor.NewGroupRouterWithPaths(
    &actor.RandomRoutingLogic{},
    []string{"/user/worker1", "/user/worker2"},
)
node.System.ActorOf(actor.Props{New: func() actor.Actor { return group }}, "workerGroup")
```

## HOCON-Driven Deployment

You can define routers directly in your HOCON configuration:

```hocon
gekka.actor.deployment {
  "/user/workerPool" {
    router = round-robin-pool
    nr-of-instances = 5
  }
  "/user/workerGroup" {
    router = random-group
    routees.paths = ["/user/w1", "/user/w2"]
  }
}
```

## Management Messages

- **`actor.Broadcast{Message: msg}`**: Delivers the inner message to *all* routees.
- **`actor.AdjustPoolSize{Delta: n}`**: (Pool only) Increases or decreases the number of routees.

---

## Typed Actor Routers (v0.10.0)

Typed Actors use classic routers via the `RouterBehavior` bridge. This allows you to leverage the full suite of routing logic while maintaining type safety for your messages.

### Using a Group Router

```go
import (
    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/actor"
)

// 1. Create the classic router
group := actor.NewGroupRouter(
    &actor.RoundRobinRoutingLogic{},
    []actor.Ref{worker1, worker2},
)

// 2. Spawn it as a typed actor using RouterBehavior
// Works with both ActorSystem and TypedContext (Integrated Spawner API)
ref, err := gekka.Spawn(system, gekka.RouterBehavior(&group.RouterActor), "myRouter")
```

### Advanced Routing Logic

Gekka supports advanced patterns compatible with Pekko/Akka:

| Logic | Description |
|-------|-------------|
| **Broadcast** | Sends every message to all routees. |
| **Scatter-Gather** | Sends a message to all routees and returns the first response within a timeout. |
| **Tail-Chopping** | Sends a message to one routee, then another if no response is received within a delay. |
| **Consistent Hashing** | Routes messages based on a hash of the message or a specific key. |

#### Scatter-Gather First Completed

```go
sg := actor.NewGroupRouter(
    &actor.ScatterGatherRoutingLogic{Within: 2 * time.Second},
    routees,
)
ref, _ := gekka.Spawn(system, gekka.RouterBehavior(&sg.RouterActor), "sgRouter")
```

#### Tail-Chopping First Completed

```go
tc := actor.NewGroupRouter(
    &actor.TailChoppingRoutingLogic{Within: 500 * time.Millisecond},
    routees,
)
ref, _ := gekka.Spawn(system, gekka.RouterBehavior(&tc.RouterActor), "tcRouter")
```

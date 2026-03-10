# Gekka Routing Features (v0.4.0)

Gekka v0.4.0 introduces robust config-driven routing, allowing you to distribute messages across multiple actors (routees) using various strategies.

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

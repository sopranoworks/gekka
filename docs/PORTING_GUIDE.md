# Porting Guide: Migrating from Pekko/Akka to Gekka 🚀

This guide assists developers familiar with **Apache Pekko** or **Lightbend Akka** in migrating their conceptual knowledge and existing systems to **Gekka**.

---

## 🔌 Wire-Level Compatibility

Gekka is designed for **full wire-level interoperability**. You can have a mixed cluster where some nodes run Pekko (Scala/Java) and others run Gekka (Go).

- **Pekko Compatibility**: Verified against **v1.1.x**.
- **Akka Compatibility**: Verified against **v2.6.21**.
- **Protocol**: Uses Artery TCP with Cumulative Demand flow control.
- **Security**: Binary-compatible with `tls-tcp` using standard PEM certificates.

---

## 🗺️ Core Concept Mapping

| Concept | Pekko/Akka (Scala) | Gekka (Go) |
|---------|-------------------|------------|
| **Entry Point** | `ActorSystem[T]` | `gekka.ActorSystem` |
| **Context** | `ActorContext[T]` | `gekka.TypedContext[T]` |
| **Behavior** | `Behavior[T]` | `gekka.Behavior[T]` |
| **Reference** | `ActorRef[T]` | `gekka.TypedActorRef[T]` |
| **Persistence** | `EventSourcedBehavior` | `gekka.EventSourcedBehavior` |
| **Sharding** | `ClusterSharding` | `cluster/sharding` subpackage |
| **Registry** | `Receptionist` | `actor/typed/receptionist` |

---

## 🐣 The Integrated Spawner API (v0.10.0)

In Pekko, there is often a friction between the "Classic" and "Typed" systems. Gekka unifies this experience through the **Integrated Spawner API**.

The `gekka.Spawn` function is a global generic that works with anything implementing the `Spawner` interface (which includes both the `ActorSystem` and the `TypedContext`).

### 1. Spawning a Top-Level Actor

**Pekko (Scala):**
```scala
val system = ActorSystem(Greeter(), "greetSystem")
// or
val ref = system.spawn(Greeter(), "greeter")
```

**Gekka (Go):**
```go
// Create system with a root behavior
system, _ := gekka.NewActorSystemWithBehavior(Greeter(), "root")

// Or spawn a top-level actor from an existing system
ref, _ := gekka.Spawn(system, Greeter(), "greeter")
```

### 2. Spawning a Child Actor

**Pekko (Scala):**
```scala
context.spawn(Child(), "child")
```

**Gekka (Go):**
```go
// Use the SAME Spawn function, just pass 'ctx' as the first argument
child, _ := gekka.Spawn(ctx, Child(), "child")
```

---

## ⚙️ Configuration

Gekka uses the same **HOCON** syntax as Pekko, powered by the [`gekka-config`](https://github.com/sopranoworks/gekka-config) engine.

- **Naming**: Gekka recognizes both `pekko.*` and `akka.*` prefixes. Standard Pekko/Akka configuration keys work directly — no need to translate to a gekka-specific namespace.
- **Cluster Settings**: All standard `pekko.cluster.*` settings are supported:
  ```hocon
  pekko.cluster {
    min-nr-of-members = 3
    retry-unsuccessful-join-after = 10s
    gossip-interval = 1s
    failure-detector {
      threshold = 8.0
      max-sample-size = 1000
      heartbeat-interval = 1s
      acceptable-heartbeat-pause = 3s
      expected-response-after = 1s
    }
  }
  ```
- **Deployment**: Routers can be configured in HOCON just like in Scala:
  ```hocon
  pekko.actor.deployment {
    "/user/myRouter" {
      router = round-robin-pool
      nr-of-instances = 5
    }
  }
  ```

---

## 💾 Persistence

Gekka's persistence model follows the Pekko Typed pattern closely.

- **Location**: Event Sourcing components have moved to `persistence/typed/` in v0.10.0.
- **Durable State**: Gekka also supports `DurableStateBehavior` for state-based persistence without event replaying.

**Migration Tip**: Ensure your `PersistenceID` strings match exactly between your Scala and Go implementations to allow seamless state handoff.

---

## 🛡️ Best Practices

1. **Location Transparency**: Always use `ref.Tell()` or `ref.Ask(ctx, ...)`. Never assume an actor is local.
2. **Type Safety**: Leverage Go generics. Avoid using `any` unless implementing a polymorphic router or supervisor.
3. **Graceful Shutdown**: Use `cluster.Shutdown()` to trigger the **Coordinated Shutdown** sequence, ensuring shards are handed off and TCP connections are closed cleanly.

---

Welcome to the Gekka ecosystem! If you encounter any "un-idiomatic" friction during your migration, please [open an issue](https://github.com/sopranoworks/gekka/issues).

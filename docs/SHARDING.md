# Cluster Sharding (v1.0.0-rc1)

Cluster Sharding distributes entities across nodes automatically. Each entity
lives on exactly one node at a time; messages are routed transparently,
regardless of where the sender or the entity lives.

---

## Concepts

| Term | Description |
|------|-------------|
| **Entity** | A stateful actor identified by a string `EntityId` |
| **Shard** | A logical group of entities; the unit of migration between nodes |
| **ShardRegion** | A local proxy that receives messages and forwards them to the correct shard/entity |
| **ShardCoordinator** | Singleton that allocates shards to regions; hosted via `ClusterSingletonProxy` |
| **ExtractEntityId** | User-supplied function that maps a message to `(EntityId, ShardId, innerMsg)` |
| **Passivation** | Removing an idle entity from memory while preserving its durable state |
| **Remember Entities** | Automatically re-spawning entities after a Shard restart by replaying a lifecycle journal |

---

## Quick Start

```go
import (
    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/actor"
    "github.com/sopranoworks/gekka/persistence"
    "github.com/sopranoworks/gekka/cluster/sharding"
)

// 1. Define commands, events, and state for the entity.
type (
    AddItem    struct { Item string `json:"item"` }
    ItemAdded  struct { Item string `json:"item"` }
    CartState  struct { Items []string }
)

// 2. Define the entity behavior using EventSourcedBehavior.
func cartBehavior(entityId string) *gekka.EventSourcedBehavior[AddItem, ItemAdded, CartState] {
    return &gekka.EventSourcedBehavior[AddItem, ItemAdded, CartState]{
        PersistenceID: "cart-" + entityId,
        Journal:       myJournal, // persistence.Journal implementation
        InitialState:  CartState{},
        CommandHandler: func(ctx gekka.TypedContext[AddItem], state CartState, cmd AddItem) gekka.Effect[ItemAdded, CartState] {
            return gekka.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})
        },
        EventHandler: func(state CartState, evt ItemAdded) CartState {
            return CartState{Items: append(state.Items, evt.Item)}
        },
    }
}

// 3. Define the shard extraction function.
extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
    switch m := msg.(type) {
    case AddItem:
        entityId := "cart-42"
        shardId  := "shard-" + string(entityId[len(entityId)-1])
        return entityId, shardId, m
    }
    return "", "", nil
}

// 4. Start sharding.
cluster, _ := gekka.NewCluster(gekka.ClusterConfig{
    SystemName: "MySystem",
    Port:       2552,
})
defer cluster.Shutdown()

entityRef, err := gekka.StartSharding[AddItem, ItemAdded, CartState](
    cluster,
    "ShoppingCart",
    cartBehavior,
    extractId,
    gekka.ShardingSettings{
        NumberOfShards: 100,
    },
)

// 5. Send a message to an entity — routing is fully transparent.
entityRef.Tell(AddItem{Item: "book"})
```

---

## ShardingSettings

```go
type ShardingSettings struct {
    // Role restricts sharding to nodes with this cluster role.
    // Empty means all nodes.
    Role string

    // NumberOfShards is the total number of shards (cannot be changed after start).
    NumberOfShards int

    // AllocationStrategy decides which node hosts a given shard.
    // Defaults to LeastShardAllocationStrategy(threshold=3, maxSimultaneous=1).
    AllocationStrategy sharding.ShardAllocationStrategy

    // PassivationIdleTimeout, when > 0, automatically stops entities that
    // have not received a message within this duration.
    //
    // HOCON: pekko.cluster.sharding.passivation.idle-timeout = 2m
    PassivationIdleTimeout time.Duration

    // RememberEntities, when true, persists EntityStarted/EntityStopped
    // events to a Journal so entities are re-spawned automatically after a
    // Shard restart or node failure.
    //
    // HOCON: pekko.cluster.sharding.remember-entities = on
    RememberEntities bool

    // Journal is used by RememberEntities to store entity lifecycle events.
    // Defaults to an InMemoryJournal when nil.  Use a durable backend
    // (PostgreSQL, Pebble, etc.) in production.
    Journal persistence.Journal
}
```

---

## Entity Passivation

Passivation removes an idle entity from memory while preserving its event-
sourced state in the journal.  When the entity receives its next message, the
Shard re-spawns it and replays the journal transparently.

### Idle-Timeout Passivation

Set `PassivationIdleTimeout` to a positive duration in `ShardingSettings`:

```go
settings := gekka.ShardingSettings{
    NumberOfShards:         100,
    PassivationIdleTimeout: 30 * time.Minute,
}
```

The Shard periodically scans for entities that have not received a message
within the timeout (check interval is `timeout/4`, minimum 500 ms) and stops
them automatically.

HOCON equivalent:

```hocon
pekko.cluster.sharding.passivation.idle-timeout = 30m
```

### Entity Self-Passivation

An entity may request its own removal by sending `actor.Passivate` to its
parent Shard through the `TypedContext.Passivate()` call:

```go
// Inside a typed actor's command handler:
ctx.Passivate()  // signals the Shard to stop this entity
```

Or, from a classic `actor.BaseActor`:

```go
func (a *MyEntity) Receive(msg any) {
    if msg == WorkDone {
        a.System()./* parent shard */Tell(actor.Passivate{Entity: a.Self()})
    }
}
```

The Shard handles `actor.Passivate`, calls `Stop` on the entity, and removes
it from its active entity table.

---

## Remember Entities

When `RememberEntities = true`, the Shard persists **EntityStarted** and
**EntityStopped** events to the configured `Journal`.  On restart (e.g., after
a node failure or shard hand-off), the Shard replays these events to
reconstruct the set of active entities and re-spawns them before processing
any new messages.

### Enabling Remember Entities

```go
journal := persistence.NewInMemoryJournal() // replace with a durable journal in production

settings := gekka.ShardingSettings{
    NumberOfShards:   100,
    RememberEntities: true,
    Journal:          journal,
}
```

HOCON equivalent:

```hocon
pekko.cluster.sharding.remember-entities = on
```

### Persistence ID

Each Shard writes to a persistence ID of the form:

```
shard-<typeName>-<shardId>
```

For example, shard `"s-0"` of the `"ShoppingCart"` type uses the ID
`"shard-ShoppingCart-s-0"`.

### Journal Requirements

- The same `persistence.Journal` instance (or a durable backend) must be
  accessible from all nodes that can host the shard.
- Use `persistence.NewInMemoryJournal()` only for tests.  For production,
  implement `persistence.Journal` against a PostgreSQL, Pebble, or other
  durable store.

### Recovery Flow

1. Shard starts (or restarts after failure).
2. `PreStart()` calls `ReadHighestSequenceNr` to find the last persisted event.
3. If events exist, `ReplayMessages` is called from sequence 1 to the highest.
4. **EntityStarted** events add the entity to the active set; **EntityStopped**
   events remove it.
5. All surviving entities are re-spawned via `entityCreator` before the Shard
   begins processing new messages.
6. Passivation tracking (`lastActivity`) is initialised for all recovered
   entities, so the idle-timeout clock starts from the moment of recovery.

---

## Combining Passivation and Remember Entities

The two features compose cleanly:

```go
settings := gekka.ShardingSettings{
    NumberOfShards:         100,
    PassivationIdleTimeout: 10 * time.Minute,
    RememberEntities:       true,
    Journal:                myDurableJournal,
}
```

Lifecycle:

| Event | Journal write | Active map |
|-------|---------------|------------|
| Entity first message | `EntityStarted` | added |
| Idle timeout / `Passivate` | `EntityStopped` | removed |
| New message to passivated entity | `EntityStarted` | re-added |
| Shard restart | _(replay)_ | rebuilt |

---

## ShardAllocationStrategy

### LeastShardAllocationStrategy (default)

Assigns new shards to the region with the fewest active shards, and
rebalances shards when the imbalance exceeds `threshold`.

```go
strategy := sharding.NewLeastShardAllocationStrategy(
    3, // rebalance threshold: max shards per region before rebalancing
    1, // max simultaneous rebalances
)
```

### Custom Strategy

Implement `sharding.ShardAllocationStrategy`:

```go
type ShardAllocationStrategy interface {
    // AllocateShard returns the ActorRef of the ShardRegion that should host shardId.
    AllocateShard(requester actor.Ref, shardId ShardId,
        currentShardAllocations map[actor.Ref][]ShardId) actor.Ref

    // Rebalance returns shards that should be migrated.
    Rebalance(currentShardAllocations map[actor.Ref][]ShardId,
        rebalanceInProgress []ShardId) []ShardId
}
```

---

## EntityRef

`EntityRef[T]` is a type-safe handle for sending messages to a specific entity:

```go
type EntityRef[T any] struct {
    EntityId EntityId // the entity's identifier
    Region   actor.Ref
}

func (r EntityRef[T]) Tell(msg T)
```

To get a reference to a specific entity from anywhere in the application:

```go
ref, err := gekka.EntityRefFor[AddItem](cluster, "ShoppingCart", "cart-42")
ref.Tell(AddItem{Item: "pen"})
```

---

## Shard Coordination

The `ShardCoordinator` is a singleton actor that tracks shard allocations. It
is automatically managed via `ClusterSingletonProxy`: when the current host
node leaves, the coordinator migrates to the next-oldest node and existing
entities are recreated there.

`ShardCoordinatorProxy` buffers messages during coordinator handover and
replays them once the new coordinator is ready.

---

## Persistence Integration

Entities created by `StartSharding` are automatically backed by
`EventSourcedBehavior`, so their state survives restarts:

- On first message: entity is created, journal is replayed.
- On subsequent messages: state is already in memory (no replay).
- On node failure: entity is recreated on another node and replays its journal.

See [PERSISTENCE.md](PERSISTENCE.md) for journal and snapshot configuration.

---

## HOCON Configuration Reference

```hocon
pekko.cluster.sharding {
    # Automatically stop entities idle longer than this duration.
    # 0 (default) disables passivation.
    passivation.idle-timeout = 30m

    # When "on", entity lifecycle events are persisted so entities are
    # re-spawned after a Shard restart.  Requires a Journal.
    remember-entities = off
}
```

---

## Serialization

Sharding uses JSON (serializer ID 9) for inter-node message routing. All
command types must be JSON-serializable. Register Pekko-compatible manifests
if the commands originate from Scala actors:

```go
import "reflect"

cluster.Serialization().RegisterManifest(
    "com.example.AddItem",
    reflect.TypeOf(AddItem{}),
)
```

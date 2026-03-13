# Cluster Sharding (v0.5.0)

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

---

## Quick Start

```go
import (
    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/actor"
    "github.com/sopranoworks/gekka/persistence"
    "github.com/sopranoworks/gekka/sharding"
)

// 1. Define commands, events, and state for the entity.
type (
    AddItem    struct { Item string `json:"item"` }
    ItemAdded  struct { Item string `json:"item"` }
    CartState  struct { Items []string }
)

// 2. Define the entity behavior using EventSourcedBehavior.
func cartBehavior(entityId string) *actor.EventSourcedBehavior[AddItem, ItemAdded, CartState] {
    return actor.NewEventSourcedBehavior[AddItem, ItemAdded, CartState]().
        WithPersistenceID("cart-" + entityId).
        WithCommandHandler(func(ctx actor.TypedContext[AddItem], state CartState, cmd AddItem) actor.Effect[ItemAdded, CartState] {
            return actor.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})
        }).
        WithEventHandler(func(state CartState, evt ItemAdded) CartState {
            return CartState{Items: append(state.Items, evt.Item)}
        })
}

// 3. Define the shard extraction function.
extractId := func(msg any) (sharding.EntityId, sharding.ShardId, any) {
    switch m := msg.(type) {
    case AddItem:
        // Use last char as shard key for demo; use consistent hashing in production.
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
    cluster.System,
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
}
```

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
ref, err := gekka.GetEntityRef[AddItem](cluster.System, "ShoppingCart", "cart-42")
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

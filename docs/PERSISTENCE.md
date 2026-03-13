# Actor Persistence (v0.5.0)

Gekka's persistence layer implements the **Event Sourcing** pattern: instead of
storing the latest state, an actor stores the sequence of *events* that led to
that state. The state is recovered at startup by replaying the stored events.

---

## Core Concepts

| Concept | Description |
|---------|-------------|
| **Command** | Input to the actor; describes *intent* |
| **Event** | Fact that was persisted; describes *what happened* |
| **State** | Derived value computed by folding events |
| **Journal** | Append-only store for events |
| **SnapshotStore** | Periodic snapshots to avoid long journal replay |
| **PersistenceID** | Unique string identifier for an actor's journal |

---

## EventSourcedBehavior

`EventSourcedBehavior[Command, Event, State]` is the core type. Provide it
with three generic parameters and two handler functions:

```go
import (
    "github.com/sopranoworks/gekka/actor"
    "github.com/sopranoworks/gekka/persistence"
)

type (
    AddItem   struct{ Item string }
    ItemAdded struct{ Item string }
    CartState struct{ Items []string }
)

behavior := actor.NewEventSourcedBehavior[AddItem, ItemAdded, CartState]().
    WithPersistenceID("cart-user-42").
    WithJournal(persistence.NewInMemoryJournal()).      // plug in any Journal
    WithSnapshotStore(persistence.NewInMemorySnapshotStore()). // optional
    WithCommandHandler(func(
        ctx   actor.TypedContext[AddItem],
        state CartState,
        cmd   AddItem,
    ) actor.Effect[ItemAdded, CartState] {
        // Validate the command, then decide what to persist.
        return actor.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})
    }).
    WithEventHandler(func(state CartState, evt ItemAdded) CartState {
        // Apply the event to compute the next state. Must be pure.
        return CartState{Items: append(state.Items, evt.Item)}
    })
```

---

## Effects

The command handler returns an `Effect` that instructs the runtime what to do:

| Effect | Description |
|--------|-------------|
| `actor.Persist(events...)` | Persist one or more events and apply them to state |
| `actor.PersistThen(then, events...)` | Persist events, then invoke `then(newState)` |
| `actor.None[E, S]()` | Acknowledge the command with no side effects |

```go
// Persist a single event.
return actor.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})

// Persist and run a callback after state is updated.
return actor.PersistThen[ItemAdded, CartState](
    func(s CartState) { log.Println("Cart now has", len(s.Items), "items") },
    ItemAdded{Item: cmd.Item},
)

// No-op — useful for read-only commands or validation failures.
return actor.None[ItemAdded, CartState]()
```

---

## Spawning a Persistent Actor

Use `SpawnPersistent` at the `ActorSystem` level:

```go
cluster, _ := gekka.NewCluster(gekka.ClusterConfig{SystemName: "MySystem", Port: 2552})

ref, err := gekka.SpawnPersistent(cluster.System, behavior, "cart-user-42")
if err != nil {
    log.Fatal(err)
}

// Type-safe messaging.
ref.Tell(AddItem{Item: "book"})
```

---

## Journal Interface

Provide any storage backend by implementing `persistence.Journal`:

```go
type Journal interface {
    // Replay stored events into callback in sequence-number order.
    ReplayMessages(ctx context.Context, persistenceId string,
        fromSequenceNr, toSequenceNr, max uint64,
        callback func(PersistentRepr)) error

    // Return the highest stored sequence number.
    ReadHighestSequenceNr(ctx context.Context, persistenceId string,
        fromSequenceNr uint64) (uint64, error)

    // Durably append a batch of events.
    AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error

    // Delete events up to (and including) toSequenceNr.
    AsyncDeleteMessagesTo(ctx context.Context, persistenceId string,
        toSequenceNr uint64) error
}
```

### Built-in: InMemoryJournal

Suitable for tests and ephemeral processes:

```go
journal := persistence.NewInMemoryJournal()
```

### Custom backend example (SQLite sketch)

```go
type SQLiteJournal struct { db *sql.DB }

func (j *SQLiteJournal) AsyncWriteMessages(ctx context.Context, msgs []persistence.PersistentRepr) error {
    for _, m := range msgs {
        data, _ := json.Marshal(m.Payload)
        _, err := j.db.ExecContext(ctx,
            `INSERT INTO events(pid, seq, payload) VALUES (?,?,?)`,
            m.PersistenceID, m.SequenceNr, data)
        if err != nil { return err }
    }
    return nil
}
// … implement remaining Journal methods
```

---

## SnapshotStore Interface

Snapshots reduce replay time for actors with long journals:

```go
type SnapshotStore interface {
    LoadSnapshot(ctx context.Context, persistenceId string,
        criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error)

    SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error

    DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error

    DeleteSnapshots(ctx context.Context, persistenceId string,
        criteria SnapshotSelectionCriteria) error
}
```

### SnapshotSelectionCriteria

```go
// Load the absolute latest snapshot:
criteria := persistence.LatestSnapshotCriteria()

// Load snapshot at or before sequence number 1000:
criteria := persistence.SnapshotSelectionCriteria{MaxSequenceNr: 1000}
```

### Built-in: InMemorySnapshotStore

```go
store := persistence.NewInMemorySnapshotStore()
```

---

## Integration with Cluster Sharding

When using `StartSharding`, each entity is automatically a persistent actor.
The sharding layer calls `SpawnPersistent` internally; supply the behavior
factory to `StartSharding`:

```go
gekka.StartSharding[AddItem, ItemAdded, CartState](
    cluster.System,
    "ShoppingCart",
    func(entityId string) *actor.EventSourcedBehavior[AddItem, ItemAdded, CartState] {
        return behavior.WithPersistenceID("cart-" + entityId)
    },
    extractId,
    gekka.ShardingSettings{NumberOfShards: 100},
)
```

See [SHARDING.md](SHARDING.md) for the full sharding reference.

---

## PersistentRepr

The internal envelope stored in the journal:

```go
type PersistentRepr struct {
    PersistenceID string
    SequenceNr    uint64
    Payload       any    // the original Event value
    Deleted       bool
    SenderPath    string
}
```

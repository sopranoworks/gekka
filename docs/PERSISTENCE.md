# Actor Persistence (v1.0.0-rc2)

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
    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/persistence"
)

type (
    AddItem   struct{ Item string }
    ItemAdded struct{ Item string }
    CartState struct{ Items []string }
)

behavior := &gekka.EventSourcedBehavior[AddItem, ItemAdded, CartState]{
    PersistenceID: "cart-user-42",
    Journal:       persistence.NewInMemoryJournal(),      // plug in any Journal
    SnapshotStore: persistence.NewInMemorySnapshotStore(), // optional
    CommandHandler: func(
        ctx   gekka.TypedContext[AddItem],
        state CartState,
        cmd   AddItem,
    ) gekka.Effect[ItemAdded, CartState] {
        // Validate the command, then decide what to persist.
        return gekka.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})
    },
    EventHandler: func(state CartState, evt ItemAdded) CartState {
        // Apply the event to compute the next state. Must be pure.
        return CartState{Items: append(state.Items, evt.Item)}
    },
}
```

---

## Effects
The command handler returns an `Effect` that instructs the runtime what to do:

| Effect | Description |
|--------|-------------|
| `gekka.Persist(events...)` | Persist one or more events and apply them to state |
| `gekka.PersistThen(then, events...)` | Persist events, then invoke `then(newState)` |
| `gekka.None[E, S]()` | Acknowledge the command with no side effects |

```go
// Persist a single event.
return gekka.Persist[ItemAdded, CartState](ItemAdded{Item: cmd.Item})

// Persist and run a callback after state is updated.
return gekka.PersistThen[ItemAdded, CartState](
    func(s CartState) { log.Println("Cart now has", len(s.Items), "items") },
    ItemAdded{Item: cmd.Item},
)

// No-op — useful for read-only commands or validation failures.
return gekka.None[ItemAdded, CartState]()
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

## Durable State

Durable State allows an actor to persist its *entire state* instead of a sequence of events. This is simpler for some use cases where the full history is not required.

### DurableStateBehavior

```go
import (
    "github.com/sopranoworks/gekka"
    "github.com/sopranoworks/gekka/persistence/typed/state"
)

type (
    Command interface{}
    Increment struct{}
    CounterState struct{ Value int }
)

behavior := gekka.DurableState[Command, CounterState](
    "counter-1",
    CounterState{Value: 0},
    myStateStore, // persistence.DurableStateStore implementation
)

behavior.OnCommand = func(
    ctx   gekka.TypedContext[Command],
    state CounterState,
    cmd   Command,
) state.Effect[CounterState] {
    switch cmd.(type) {
    case Increment:
        return state.Persist(CounterState{Value: state.Value + 1})
    }
    return state.None()
}
```

### Spawning a Durable State Actor

```go
ref, err := gekka.SpawnDurableState(cluster.System, behavior, "counter-1")
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
    func(entityId string) *gekka.EventSourcedBehavior[AddItem, ItemAdded, CartState] {
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

# examples/crdt-dashboard — Live Cluster State Dashboard

Demonstrates Gekka's CRDT Replicator with a live terminal dashboard that
updates every 2 seconds, showing **eventual consistency** in action.

| CRDT | Key | Semantics |
|------|-----|-----------|
| GCounter | `total_requests` | Grows monotonically; global value = sum of all nodes |
| ORSet | `active_nodes` | Members add themselves on join, remove on graceful leave |

---

## How it works

```
┌─────────────────────────────────────────────────────────┐
│  Go dashboard-1 ─┐                                      │
│  Go dashboard-2 ─┼──► Scala GoReplicator ──► merge      │
│  Go dashboard-N ─┘        │                  + reply    │
│                            └──► back to each Go node    │
└─────────────────────────────────────────────────────────┘
```

Each Go instance gossips its local CRDT state to the Scala `GoReplicator`
actor every 2 s. Scala merges all contributions and replies with the union.
The Go node merges the reply into its local state. After one round-trip each
Go dashboard reflects the combined state of all running instances.

**Eventual consistency is visible**: launch two terminals running this
example, increment `total_requests` on both, and watch each dashboard
converge to the same global sum within one gossip interval.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.23+ |
| sbt | 1.x |
| Apache Pekko node running a `GoReplicator` actor at `/user/goReplicator` | Pekko 1.x |

---

## Running the example

### 1. Start the Scala GoReplicator server

The companion server lives in `../../scala-server`:

```bash
cd ../../scala-server
sbt "runMain com.example.DistributedDataServer"
```

Wait until you see:

```
--- DISTRIBUTED DATA SERVER READY ---
```

`DistributedDataServer` starts a `GoReplicator` actor at `/user/goReplicator`
that understands the Gekka JSON gossip envelope:

```json
{"type":"gcounter-gossip","key":"total_requests","payload":{"state":{"127.0.0.1:54321":3}}}
{"type":"orset-gossip",   "key":"active_nodes",  "payload":{...}}
```

### 2. Run the Go dashboard

```bash
go run ./examples/crdt-dashboard
```

### 3. (Optional) Run a second dashboard instance to observe convergence

Open a new terminal:

```bash
go run ./examples/crdt-dashboard
```

Each instance gets a unique OS-assigned port, so they coexist without
conflict.  After one gossip round-trip you will see both instances appear
in the `active_nodes` ORSet, and both `total_requests` values climbing.

---

## Sample dashboard output

```
┌────────────────────────────────────────────────────────┐
│            Cluster CRDT Dashboard                      │
│            system: ClusterSystem                       │
│            node:   127.0.0.1:54321                     │
├────────────────────────────────────────────────────────┤
│   GCounter — total_requests                            │
│     Global value : 47                                  │
│     My increment : 23                                  │
├────────────────────────────────────────────────────────┤
│   ORSet — active_nodes                                 │
│     127.0.0.1:54321  ← this node                      │
│     127.0.0.1:54987                                    │
├────────────────────────────────────────────────────────┤
│   Updated : 2026-03-08 12:00:04                        │
│   Gossip  : every 2s                                   │
└────────────────────────────────────────────────────────┘
  Press Ctrl-C to leave gracefully.
```

---

## CRDT semantics

### GCounter

Each node owns one slot keyed by its `host:port`. The global value is the
sum of all slots. Merging is pairwise-max, so values are never lost — even
during a network partition, both sides keep incrementing, and the full total
is recovered when they reconnect.

```
node-A: {A:10, B:0}      node-B: {A:0,  B:7}
         ──── merge ────►
result:  {A:10, B:7}   value = 17
```

### ORSet (Observed-Remove Set)

Each `Add` is tagged with a unique `(nodeID, counter)` dot. `Remove` clears
all observed dots for that element. Concurrent adds from different nodes
survive: if node-A adds "x" while node-B removes "x", "x" stays in the set
(add wins over concurrent remove).

---

## Gossip interval

The default gossip interval is 2 s (matching `refreshInterval` in `main.go`).
Reduce it for faster convergence in demos:

```go
repl.GossipInterval = 500 * time.Millisecond
```

Or keep it at 2 s to make the eventual-consistency delay clearly observable.

---

## Graceful shutdown

When you press Ctrl-C, the dashboard:

1. Calls `repl.RemoveFromSet("active_nodes", myAddr, crdt.WriteAll)` — pushes the
   remove to the Scala hub immediately so other nodes stop advertising this
   address within the next gossip cycle.
2. Calls `node.Leave()` — broadcasts a Leave message through the cluster so
   Pekko's SBR cleanly transitions this node to Removed.

---

## Next steps

- **Distributed worker** — route jobs to a cluster singleton with automatic
  failover; see `examples/distributed-worker/`
- **crdt.WriteAll consistency** — change `crdt.WriteLocal` to `crdt.WriteAll` in
  `IncrementCounter` / `AddToSet` calls to push updates immediately rather
  than waiting for the next gossip tick
- **OR-Set tags** — store feature flags or service capabilities in the ORSet
  and read them from any node in the cluster

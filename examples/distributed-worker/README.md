# examples/distributed-worker — Cluster Singleton Job Dispatcher

Demonstrates routing work to a **Pekko `ClusterSingletonManager`** from Go
using `node.SingletonProxy`, including **automatic failover** when the oldest
cluster member changes.

| Step | API | What it does |
|------|-----|--------------|
| 1 | `SpawnFromConfig("application.conf")` | Parse HOCON, bind TCP listener (port 0) |
| 2 | `node.System.ActorOf(Props{...}, "ackReceiver")` | Receive JSON acknowledgements from the singleton |
| 3 | `node.JoinSeeds()` + `WaitForHandshake` | Join 3-node cluster, await Artery handshake |
| 4 | `node.SingletonProxy("/user/singletonManager", "")` | Create a proxy to the singleton |
| 5 | `proxy.Send(ctx, jobBytes)` | Dispatch jobs; re-routes automatically on failover |

---

## Cluster topology

```
┌──────────────────────────────────────────────────┐
│ 3-node cluster                                   │
│                                                  │
│  Scala seed-1 :2552  ← oldest → runs singleton  │
│  Scala seed-2 :2553  ← takes over on failover   │
│  Go worker    :OS    ← this example              │
└──────────────────────────────────────────────────┘
```

When **seed-1 is stopped**, Pekko's SBR marks it unreachable → the singleton
migrates to seed-2 → the Go proxy routes the next `Send` to seed-2
automatically, with zero code changes.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.23+ |
| sbt | 1.x |
| Apache Pekko cluster seed running `ClusterSingletonManager` | Pekko 1.x |

---

## Running the example

### 1. Start Scala seed-1 (port 2552) — hosts the singleton

The companion singleton server lives in `../../scala-server`:

```bash
cd ../../scala-server
sbt "runMain com.example.ClusterSingletonServer"
```

Wait until you see:

```
--- CLUSTER SINGLETON READY ---
```

`ClusterSingletonServer` creates a `ClusterSingletonManager` at
`/user/singletonManager`. Its child singleton actor responds to every
`Array[Byte]` message with `"Ack: <original>"`.

### 2. (Optional) Start Scala seed-2 (port 2553) for failover testing

Open a second terminal and start another instance, or a standard
`ClusterSeedNode` that also bootstraps as a seed:

```bash
sbt "runMain com.example.ClusterSeedNode"
```

Adjust `ClusterSeedNode.scala` to listen on port 2553 if needed, or set
`pekko.remote.artery.canonical.port = 2553` in that project's config.

### 3. Run the Go worker

From the repository root:

```bash
go run ./examples/distributed-worker
```

### Expected output

```
[worker] listening on 127.0.0.1:54801
[worker] joining cluster via pekko://ClusterSystem@127.0.0.1:2552 …
[worker] joined cluster. Waiting for membership to converge …
[worker] singleton → pekko://ClusterSystem@127.0.0.1:2552/user/singletonManager/singleton
[worker] → dispatched job-0001
[worker] <- received (serializerId=4): "Ack: {"id":"job-0001",…}"
[worker] singleton → pekko://ClusterSystem@127.0.0.1:2552/user/singletonManager/singleton
[worker] → dispatched job-0002
…
```

---

## Observing automatic failover

With both Scala seeds running:

1. Note the `singleton →` line — it ends with `:2552/user/singletonManager/singleton`.
2. **Stop** the seed-1 process (`Ctrl-C` in its terminal).
3. Watch the Go worker log — after the SBR `stable-after` period (10 s by default)
   the singleton line changes to `:2553/…` automatically.
4. Jobs continue flowing without any application code change.

To speed up failover during development, reduce `stable-after` in
`application.conf`:

```hocon
pekko.cluster.split-brain-resolver.stable-after = 5s
```

---

## Singleton manager path

The proxy is constructed with:

```go
proxy := node.SingletonProxy("/user/singletonManager", "")
```

`/user/singletonManager` is the path passed to `system.actorOf(…, "singletonManager")`
on the Scala side. The proxy appends `/singleton` (Pekko's default child name):

```
pekko://ClusterSystem@<oldest-host>:<oldest-port>/user/singletonManager/singleton
```

Pass a role string (e.g. `"backend"`) as the second argument to restrict
routing to nodes tagged with that cluster role in their Pekko config.

---

## Job / Ack schema

```go
// Sent to the singleton (JSON-encoded []byte)
type Job struct {
    ID      string `json:"id"`
    Payload string `json:"payload"`
    SentAt  string `json:"sent_at"`
}

// Expected reply from the Scala singleton
type Ack struct {
    JobID   string `json:"job_id"`
    Status  string `json:"status"`
    Message string `json:"message,omitempty"`
}
```

Adapt the schema to match your Scala singleton's actual message protocol.

---

## Next steps

- **CRDT dashboard** — track cluster-wide counters and active-node lists;
  see `examples/crdt-dashboard/`
- **Multiple workers** — run several instances of this example at once; the
  singleton receives jobs from all of them and replies independently
- **Role-based routing** — tag Scala nodes with a cluster role and pass it
  to `SingletonProxy` to restrict where the singleton can live

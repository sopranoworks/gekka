# Split Brain Resolver (SBR)

A network partition splits a cluster into two or more disconnected groups.
Without a resolver each partition believes the other is dead and may promote
a new leader — resulting in a "split brain" where both sides continue to
operate independently.  Pekko/Akka's **Split Brain Resolver** (SBR) addresses
this by letting each partition independently decide which side survives.

## How it works

1. The **Phi Accrual Failure Detector** in each node continuously monitors
   heartbeat arrival times.  When phi exceeds the configured threshold the
   detected member is marked **Unreachable** and an `UnreachableMember` event
   is published.

2. The **SBRManager** listens for `UnreachableMember` events and starts a
   **stable-after** timer.  The timer resets on every new `UnreachableMember`
   event and cancels when all members become reachable again.

3. When the stable-after timer fires without a recovery event the SBRManager
   calls `classifyMembers()` to build a reachable/unreachable view of all
   `Up` and `WeaklyUp` members, then passes this to the configured
   **Strategy**.

4. The strategy returns a `Decision`:
   - `DownSelf = true` → this node should gracefully leave the cluster.
   - `DownMembers` → the leader should mark these members as `Down`.
     The normal gossip leader-actions loop then transitions `Down → Removed`.

## Strategies

### keep-majority (default)

Keeps the partition that has more than half of the Up/WeaklyUp members.

**Tie-break** (equal partition sizes): the side containing the member with
the lexicographically lowest `host:port` address survives.

```hocon
pekko.cluster.split-brain-resolver {
  active-strategy = keep-majority
  stable-after    = 20s
}
```

### keep-oldest

Keeps the partition that contains the oldest member (i.e. the member with
the lowest `upNumber`, which corresponds to the earliest join time).

When `down-if-alone = on` and the oldest member is the sole survivor on its
side of the partition, it downs itself rather than forming a singleton cluster.

```hocon
pekko.cluster.split-brain-resolver {
  active-strategy = keep-oldest
  stable-after    = 20s
  keep-oldest {
    down-if-alone = on
    role          = ""      # empty = all roles
  }
}
```

### keep-referee

Keeps the partition that can reach a designated **referee** node.  The
referee is identified by its `host:port`.

```hocon
pekko.cluster.split-brain-resolver {
  active-strategy = keep-referee
  stable-after    = 20s
  keep-referee {
    referee = "10.0.0.100:2551"
  }
}
```

### static-quorum

Requires at least `quorum-size` reachable members to survive.

```hocon
pekko.cluster.split-brain-resolver {
  active-strategy = static-quorum
  stable-after    = 20s
  static-quorum {
    quorum-size = 3
    role        = ""    # empty = all roles
  }
}
```

### lease-majority

Like `keep-majority`, but every survivor must additionally **acquire a
distributed lease** before downing the unreachable side.  The lease is the
authoritative tiebreaker — only one partition can hold it at a time, so the
strategy is safe even when both sides see equal partition sizes (the case
where `keep-majority` falls back to a lexicographic tiebreaker).

When the local side is in the **minority** the strategy waits
`acquire-lease-delay-for-minority` before attempting `Acquire`, giving the
majority side a head-start to acquire the lease first.  If the lease cannot
be acquired the side downs itself.

```hocon
pekko.cluster.split-brain-resolver {
  active-strategy = lease-majority
  stable-after    = 20s
  lease-majority {
    lease-implementation              = ""   # empty → in-memory ref provider
    acquire-lease-delay-for-minority  = 2s
    role                              = ""   # empty = all roles
  }
}
```

`lease-implementation` is the name of a `LeaseProvider` registered on the
`LeaseManager` (defaults to gekka's in-memory `lease.MemoryProviderName`).
`Cluster.NewCluster` auto-wires `LeaseManager`, `LeaseSettings.LeaseName`
(`<system>-pekko-sbr`) and `LeaseSettings.OwnerName` (`<host>:<port>`) when
the active strategy is `lease-majority`, so a HOCON-only deployment lights
up the in-memory provider out of the box.

## Configuration reference

| HOCON key | Go field | Default | Notes |
|---|---|---|---|
| `active-strategy` | `SBRConfig.ActiveStrategy` | `""` (disabled) | One of the five strategy names |
| `stable-after` | `SBRConfig.StableAfter` | `20s` | How long reachability must remain stable |
| `down-all-when-unstable` | `SBRConfig.DownAllWhenUnstable` | `on` | Down all nodes if instability persists beyond stable-after + derived duration |
| `keep-majority.role` | `SBRConfig.Role` | `""` | Count only members with this role |
| `keep-oldest.role` | `SBRConfig.Role` | `""` | Count only members with this role |
| `keep-oldest.down-if-alone` | `SBRConfig.DownIfAlone` | `false` | |
| `keep-referee.referee` | `SBRConfig.RefereeAddress` | `""` | `host:port` of the referee |
| `static-quorum.quorum-size` | `SBRConfig.QuorumSize` | `0` | Minimum reachable members |
| `lease-majority.lease-implementation` | `SBRConfig.LeaseImplementation` | `""` | LeaseProvider name; resolved via `cfg.CoordinationLease.LeaseManager` (defaults to in-memory) |
| `lease-majority.acquire-lease-delay-for-minority` | `SBRConfig.AcquireLeaseDelayForMinority` | `2s` | Minority-side delay before `Acquire` |
| `lease-majority.role` | `SBRConfig.LeaseMajorityRole` | `""` | Count only members with this role; falls back to `SBRConfig.Role` |

## Programmatic configuration

```go
node, err := gekka.NewCluster(gekka.ClusterConfig{
    SystemName: "ClusterSystem",
    Host:       "127.0.0.1",
    Port:       2553,
    SBR: gekka.SBRConfig{
        ActiveStrategy: "keep-majority",
        StableAfter:    20 * time.Second,
    },
})
```

## Integration with Coordinated Shutdown

When `DownSelf` is returned the SBRManager calls `LeaveCluster()`, which
triggers the normal Coordinated Shutdown sequence (if wired) and drives the
node through `Leaving → Exiting → Removed`.

## Implementation files

| File | Purpose |
|------|---------|
| `cluster/sbr_strategy.go` | `Strategy` interface, `SBRConfig`, `Member`, `Decision`, all four strategy implementations |
| `cluster/sbr_manager.go` | `SBRManager` goroutine, `SubscribeChannel`, `classifyMembers`, `ChanSubscription` |
| `cluster/cluster_manager.go` | `DownMember()`, `Down → Removed` leader transition, `LeaveCluster` local state update |
| `cluster.go` | `SBRConfig` type alias, `NewCluster` SBR wiring, `MuteNode`/`UnmuteNode`/`SubscribeChannel` |
| `internal/core/association.go` | `MuteNode`/`UnmuteNode` on `NodeManager`; mute checks in send and dispatch paths |
| `hocon_config.go` | `split-brain-resolver.*` HOCON parsing, `parseHOCONDuration` |
| `cluster/sbr_test.go` | Unit tests for all strategies |
| `integration_sbr_test.go` | E2E integration tests against `SBRTestNode` |
| `scala-server/…/SBRTestNode.scala` | Pekko cluster node with fast FD settings for SBR integration tests |

## Interoperability Testing

### How Gekka and Pekko coordinate SBR decisions via gossip

Both Gekka and Pekko run their SBR logic **independently** on each partition.
They do not negotiate: each side uses only the reachability information it can
observe locally, applies its configured strategy, and acts on the result.

```
  [Scala/Pekko]   <── gossip ──>   [Go/Gekka]
        │                               │
   phi-accrual FD                phi-accrual FD
        │                               │
   stable-after                   stable-after
        │                               │
   Decide (Strategy)             Decide (Strategy)
        │                               │
  Down Go node              DownSelf → LeaveCluster
```

For a **2-node cluster** the strategies reach complementary decisions:

| Strategy | Scala sees | Go sees | Outcome |
|---|---|---|---|
| keep-majority (tie) | Go unreachable → survive | Scala unreachable → DownSelf | Scala downs Go; Go leaves |
| keep-oldest | oldest reachable → survive | oldest unreachable → DownSelf | both converge |

### Network partition simulation

Integration tests use `Cluster.MuteNode(host, port)` to silently drop all
outbound frames to the target node and discard all inbound frames from it.
This simulates a **bidirectional network partition** without terminating either
process. The phi-accrual failure detector on each side independently detects
the loss of heartbeats and raises an `UnreachableMember` event after
`acceptable-heartbeat-pause`.

```go
// Simulate partition between Go and Scala
node.MuteNode("127.0.0.1", 2552)

// Restore connectivity (e.g. for recovery tests)
node.UnmuteNode("127.0.0.1", 2552)
```

### Observing SBR decisions

On the **Scala side**, `SBRTestNode` prints structured signal lines that the
Go test reads via stdout:

```
SBR_NODE_READY
PEKKO_MEMBER_UP:127.0.0.1:2553
PEKKO_MEMBER_UNREACHABLE:127.0.0.1:2553   ← FD fired
PEKKO_MEMBER_DOWN:127.0.0.1:2553          ← SBR downed Go
PEKKO_MEMBER_REMOVED:127.0.0.1:2553       ← leader cleaned up
```

On the **Go side**, subscribe to `gcluster.UnreachableMember` and
`gcluster.MemberLeft` via `Cluster.SubscribeChannel`:

```go
sub := node.SubscribeChannel(
    reflect.TypeOf(gcluster.UnreachableMember{}),
    reflect.TypeOf(gcluster.MemberLeft{}),
)
defer sub.Cancel()

for evt := range sub.C {
    switch e := evt.(type) {
    case gcluster.UnreachableMember:
        log.Printf("unreachable: %s", e.Member)
    case gcluster.MemberLeft:
        log.Printf("left: %s", e.Member)  // emitted by SBR DownSelf
    }
}
```

### Running the SBR integration tests

```bash
# Both tests (requires sbt in PATH and a free port 2552/2553)
go test -v -tags integration -run TestSBRKeepMajorityInterop -timeout 300s .
go test -v -tags integration -run TestSBRKeepOldestInterop   -timeout 300s .

# Or together
go test -v -tags integration -run 'TestSBR' -timeout 300s .
```

Expected runtime: ~30–60 s per test (FD detection ≈ 3 s + stable-after ≈ 2 s + gossip convergence).

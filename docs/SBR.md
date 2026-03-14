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

## Configuration reference

| HOCON key | Go field | Default | Notes |
|---|---|---|---|
| `active-strategy` | `SBRConfig.ActiveStrategy` | `""` (disabled) | One of the four strategy names |
| `stable-after` | `SBRConfig.StableAfter` | `20s` | How long reachability must remain stable |
| `keep-majority.role` | `SBRConfig.Role` | `""` | Count only members with this role |
| `keep-oldest.role` | `SBRConfig.Role` | `""` | Count only members with this role |
| `keep-oldest.down-if-alone` | `SBRConfig.DownIfAlone` | `false` | |
| `keep-referee.referee` | `SBRConfig.RefereeAddress` | `""` | `host:port` of the referee |
| `static-quorum.quorum-size` | `SBRConfig.QuorumSize` | `0` | Minimum reachable members |

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
| `cluster/cluster_manager.go` | `DownMember()`, `Down → Removed` leader transition |
| `cluster.go` | `SBRConfig` type alias, `NewCluster` SBR wiring |
| `hocon_config.go` | `split-brain-resolver.*` HOCON parsing, `parseHOCONDuration` |
| `cluster/sbr_test.go` | Unit tests for all strategies |

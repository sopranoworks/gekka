<!--
  MULTI_DC.md — Gekka Multi-Data-Center Support
  Copyright (c) 2026 Sopranoworks, Osamu Takahashi
  SPDX-License-Identifier: MIT
-->

# Multi-Data-Center (Multi-DC) Cluster Support (v0.7.0)

Gekka supports multi-data-center cluster topologies, matching Apache Pekko's
`pekko.cluster.multi-data-center` feature.  Each node declares its own data
center at startup; the label is carried through the cluster gossip as a
`"dc-<name>"` role, making it visible to membership queries, singletons, and
shard allocation.

---

## Concepts

| Term | Description |
|------|-------------|
| **Data Center (DC)** | A logical grouping label for cluster nodes, e.g. `"us-east"`, `"eu-west"`. Encoded as role `"dc-<name>"` in gossip. |
| **self-data-center** | The DC label of the local node. Defaults to `"default"`. |
| **OldestNodeInDC** | Selects the node with the lowest `upNumber` within a specific DC. Used by `ClusterSingletonManager` to scope singleton placement. |
| **MembersInDataCenter** | Returns all Up/WeaklyUp members belonging to a DC. |
| **IsInDataCenter** | Reports whether a node (host:port) is a member of a given DC. |

---

## Quick Start

### Programmatic configuration

```go
node, err := gekka.NewCluster(gekka.ClusterConfig{
    SystemName: "ClusterSystem",
    Host:       "10.0.1.10",
    Port:       2552,
    DataCenter: "us-east",       // ← new field
})
```

### HOCON configuration

```hocon
pekko {
  remote.artery.canonical {
    hostname = "10.0.1.10"
    port     = 2552
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@10.0.0.1:2552"]
    multi-data-center {
      self-data-center = "us-east"    # default: "default"
    }
  }
}
```

Load via the standard `gekka.LoadConfig`:

```go
node, err := gekka.NewClusterFromConfig("application.conf")
```

---

## ClusterManager DC Queries

Once the cluster is running, use the following `ClusterManager` methods:

```go
cm := node.ClusterManager() // exposed via *Cluster

// Oldest Up member in a DC (compatible with ClusterSingleton selection).
ua := cm.OldestNodeInDC("us-east", "") // second arg = optional role filter

// All Up/WeaklyUp members in a DC.
members := cm.MembersInDataCenter("us-east")
for _, m := range members {
    fmt.Printf("%s:%d (DC=%s)\n", m.Host, m.Port, m.DataCenter)
}

// Check if a specific node is in a DC.
if cm.IsInDataCenter("10.0.1.10", 2552, "us-east") {
    // same DC
}
```

### Helper: extract DC from gossip member

```go
dc := cluster.DataCenterForMember(gossip, member)
// Returns "us-east" for a member with role "dc-us-east"; "default" if absent.
```

---

## DC-Scoped ClusterSingleton

Scope a `ClusterSingletonManager` to a specific data center so the singleton
only runs on the oldest node **within that DC**:

```go
import "github.com/sopranoworks/gekka/cluster/singleton"

mgr := singleton.NewClusterSingletonManager(cm, actor.Props{
    New: func() actor.Actor { return &MySingleton{} },
}, "") // role filter (empty = any)
mgr.WithDataCenter("us-east")  // restrict to us-east DC
```

Similarly, scope a `ClusterSingletonProxy`:

```go
proxy := singleton.NewClusterSingletonProxy(cm, router, "/user/mySingletonManager", "")
proxy.WithDataCenter("us-east")
```

---

## DC-Aware Cluster Sharding

Restrict shard allocation to a specific data center:

```go
entityRef, err := gekka.StartSharding[Command, Event, State](
    cluster,
    "ShoppingCart",
    cartBehavior,
    extractId,
    gekka.ShardingSettings{
        NumberOfShards: 100,
        DataCenter:     "us-east",    // only allocate shards to us-east nodes
    },
)
```

When `DataCenter` is set:
- `ShardSettings.IsLocalDC` is populated automatically with a closure that
  calls `cm.IsInDataCenter(host, port, dc)`.
- Custom `ShardAllocationStrategy` implementations can use
  `settings.IsLocalDC(host, port)` to prefer local-DC nodes.

### Custom DC-aware allocation strategy

```go
type DCFirstAllocationStrategy struct {
    isLocalDC func(host string, port uint32) bool
    fallback  sharding.ShardAllocationStrategy
}

func (s *DCFirstAllocationStrategy) AllocateShard(
    requester actor.Ref,
    shardId sharding.ShardId,
    current map[actor.Ref][]sharding.ShardId,
) actor.Ref {
    // Prefer a region whose node is in the local DC.
    for region := range current {
        host, port := parseHostPort(region.Path())
        if s.isLocalDC(host, port) {
            return region
        }
    }
    return s.fallback.AllocateShard(requester, shardId, current)
}
```

---

## Wire Encoding

Pekko represents data centers as cluster roles with the `"dc-"` prefix.  When
a Go node joins with `DataCenter = "us-east"`, Gekka sends:

```protobuf
Join {
  node  = <UniqueAddress>
  roles = ["dc-us-east"]
}
```

This is stored in `Gossip.AllRoles` and referenced by `Member.RolesIndexes`, so
DC membership is visible to every node in the cluster — including Scala/Pekko
peers — without any additional wire-format changes.

---

## HOCON Reference

```hocon
pekko.cluster.multi-data-center {
  # This node's data-center label.  Must match the "dc-<name>" role that
  # other nodes in the same DC will also carry.
  # Default: "default"
  self-data-center = "us-east"
}
```

---

## Compatibility

- Wire-compatible with Apache Pekko 1.x and Lightbend Akka 2.6+ multi-DC
  clusters: Go nodes send `dc-<name>` roles in `Join` messages just as Scala
  nodes do.
- A Pekko Scala node in `"dc-us-east"` and a Gekka Go node configured with
  `DataCenter: "us-east"` will correctly share DC membership in the gossip.

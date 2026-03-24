# Operational Tooling Guide (v0.13.0)

Two standalone binaries ship with the gekka module for cluster operators:
**`gekka-cli`** for interactive cluster management, and **`gekka-metrics`** for
real-time observability via OpenTelemetry.

---

## Table of Contents

- [gekka-cli](#gekka-cli)
  - [Installation](#installation)
  - [Global Flags and Config File](#global-flags-and-config-file)
  - [Cluster Management](#cluster-management)
  - [Shard Management](#shard-management)
  - [Interactive Dashboard](#interactive-dashboard)
  - [Discovery Diagnostics](#discovery-diagnostics)
- [gekka-metrics](#gekka-metrics)
  - [Integration with Prometheus / Grafana](#integration-with-prometheus--grafana)
  - [Key Metrics Reference](#key-metrics-reference)
- [Distributed Tracing](#distributed-tracing)

---

## gekka-cli

`gekka-cli` is a command-line interface for inspecting and managing a live
cluster via the **HTTP Management API**.

### Installation

Build from source (requires Go 1.26.1+):

```bash
go install github.com/sopranoworks/gekka/cmd/gekka-cli@latest
```

Or build the binary locally:

```bash
go build -o gekka-cli ./cmd/gekka-cli
```

### Global Flags and Config File

```
gekka-cli [--config FILE] [--profile NAME] [--json] <subcommand>
```

| Flag | Default | Description |
|---|---|---|
| `--config FILE` | `~/.gekka/config.yaml` | Path to the config file |
| `--profile NAME` | *(default_profile)* | Named profile to select from the config file |
| `--json` | false | Output raw JSON instead of a formatted table |

**Management URL resolution order** (first match wins):

1. `--url` flag on the subcommand
2. `--profile` flag → that profile's `management_url` in the config file
3. `default_profile` → that profile's `management_url` in the config file
4. Top-level `management_url` in the config file
5. Built-in default: `http://127.0.0.1:8558`

**Config file** (`~/.gekka/config.yaml`):

```yaml
management_url: http://127.0.0.1:8558
default_profile: local

profiles:
  local:
    management_url: http://127.0.0.1:8558
  staging:
    management_url: http://staging-node:8558
  prod:
    management_url: http://prod-seed-1:8558
```

---

### Cluster Management

#### `members` — List cluster members

Calls `GET /cluster/members` and prints a human-readable table of every known
node with its address, status, data-center, roles, reachability, and RTT.

```bash
gekka-cli members
gekka-cli members --url http://prod-seed-1:8558
gekka-cli members --profile prod --json
```

**Example output:**

```
ADDRESS                          STATUS  DC       ROLES                REACHABLE  RTT
-------                          ------  --       -----                ---------  ---
pekko://System@10.0.0.1:2552     Up      dc1      seed                 yes        2ms
pekko://System@10.0.0.2:2552     Up      dc1      cart,checkout        yes        4ms
pekko://System@10.0.0.3:2552     Up      dc2      cart                 yes        18ms
pekko://System@10.0.0.4:2552     Leaving dc1      seed                 yes        3ms
```

Status colors:
- **Green** — `Up` / `Joining`
- **Red** — unreachable nodes
- **Amber / Red** RTT — latency warnings (≥50 ms, ≥200 ms, ≥500 ms)

---

### Shard Management

The `shards` subcommand provides visibility into and control over
**Cluster Sharding** state.  Both sub-commands target a specific entity type
(the `typeName` registered with `NewShardRegion`).

```
gekka-cli shards [--url URL] <list|rebalance> ...
```

#### `shards list <typeName>` — View shard distribution

Calls `GET /cluster/sharding/{typeName}` and prints the current
shard-to-region assignment table.

```bash
# Show shard distribution for entity type "Cart"
gekka-cli shards list Cart

# Scope to a specific cluster node
gekka-cli shards list Cart --url http://node-1:8558

# Get raw JSON (useful for scripting)
gekka-cli shards list Cart --json
```

**Example output:**

```
SHARD ID   REGION PATH
--------   -----------
shard-0    /user/Cart-region
shard-1    /user/Cart-region
shard-2    /user/Cart-region#2
shard-3    /user/Cart-region#2
...
```

Use this command to identify hotspots or verify rebalancing progress after a
node joins or leaves.

#### `shards rebalance <typeName> <shardID> <targetRegion>` — Move a shard

Calls `POST /cluster/sharding/{typeName}/rebalance` to initiate a
manual handoff of a specific shard to a target `ShardRegion`.

```bash
# Move shard-3 of type "Cart" to the region on node-2
gekka-cli shards rebalance Cart shard-3 /user/Cart-region#2
```

`<targetRegion>` must be the **full actor path** of a registered `ShardRegion`
(e.g. `/user/Cart-region`).  The shard coordinator will begin the
`BeginHandOff → HandOff → ShardStopped` sequence; in-flight messages are
buffered and replayed on the new host.

> **Note:** Manual rebalancing is intended for operational emergencies.  Under
> normal conditions the shard coordinator automatically rebalances when the
> cluster membership changes.

---

### Interactive Dashboard

The `dashboard` command opens a terminal UI showing a live view of cluster
member health, refreshed every few seconds.

```bash
gekka-cli dashboard
gekka-cli dashboard --url http://staging-node:8558
```

Features:
- **Automatic Alignment** — Dynamic column widths ensure Artery addresses and RTTs are perfectly aligned.
- **Roles Marquee** — Member roles exceeding 20 characters automatically scroll within their cell to preserve layout.
- **Local Filter** — The dashboard automatically identifies and hides the local node to focus on remote peers.

**Exit Behavior:**
Pressing `q` or `ESC` triggers a confirmation overlay. Press `y` to exit or `n` to return to the dashboard. The overlay automatically clears after 5 seconds of inactivity.

**Example output:**

```
ADDRESS                          STATUS  ROLES                   REACHABLE  RTT
-------                          ------  -----                   ---------  ---
pekko://System@10.0.0.1:2552     Up      seed,shard-host         yes        2ms
pekko://System@10.0.0.2:2552     Up      worker,compute          yes        4ms
pekko://System@10.0.0.3:2552     Up      api-gateway,ingress     yes        12ms
pekko://System@10.0.0.4:2552     Down    -                       NO         timeout
```

---

### Discovery Diagnostics

`discovery-check` is a diagnostic tool for verifying Kubernetes API or DNS
service discovery settings before connecting to a real cluster.

```bash
gekka-cli discovery-check
gekka-cli discovery-check --url http://127.0.0.1:8558
```

It reports whether the configured discovery mechanism can reach the expected
seed addresses, which is useful when debugging bootstrap failures in
Kubernetes environments.

---

## gekka-metrics

`gekka-metrics` is a **full cluster node** that joins the ring with the
`metrics-exporter` role and reads gossip state directly — no HTTP polling, no
dependency on the Management API.  It exports cluster metrics via the
**OpenTelemetry Protocol (OTLP/HTTP)**.

Because `gekka-metrics` is a real cluster node it sees membership changes in
real time via gossip, giving sub-second lag between a node going unreachable
and the metric updating.

### Installation

```bash
go install github.com/sopranoworks/gekka/cmd/gekka-metrics@latest
# or
go build -o gekka-metrics ./cmd/gekka-metrics
```

### Running

```bash
gekka-metrics --config cluster.conf [--otlp http://otel-collector:4318]
```

| Flag | Default | Description |
|---|---|---|
| `--config FILE` | *(required)* | Path to a HOCON cluster config |
| `--otlp ENDPOINT` | *(empty — local only)* | OTLP/HTTP endpoint to push metrics to |

**Minimal HOCON config:**

```hocon
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port     = 2560
  }
  cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
}

gekka.telemetry.exporter.otlp {
  endpoint = "http://otel-collector:4318"
}
```

The `metrics-exporter` role is injected automatically so sharding allocators
and singleton managers exclude this node from hosting production workloads.

When no OTLP endpoint is configured the process still joins the cluster and
displays a live TUI view of membership state.

**Log Verbosity:**
By default, `gekka-metrics` suppresses high-frequency protocol logs (heartbeats, gossip status, etc.) to keep the TUI clear. To enable detailed protocol tracing, set `gekka.logging.level = "DEBUG"` in your HOCON configuration.

---

### Integration with Prometheus / Grafana

Use the **OpenTelemetry Collector** as the bridge between `gekka-metrics` and
Prometheus:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: "0.0.0.0:4318"

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
  debug:
    verbosity: basic

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus, debug]
```

Then add the Prometheus scrape target:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: gekka
    static_configs:
      - targets: ['otel-collector:8889']
```

Import the Gekka community Grafana dashboard (ID **TBD**) or build your own
using the metric names in the reference below.

---

### Key Metrics Reference

#### Currently Exported

| Metric | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `gekka.cluster.members` | ObservableGauge | `{members}` | `status`, `dc` | Number of cluster members grouped by status and data-center |

**Attribute values for `status`:** `up`, `joining`, `leaving`, `exiting`, `down`, `weakly-up`, `removed`

#### Planned Metrics (v0.13.0+)

The following metrics are on the roadmap and will be added in a future release:

| Metric | Type | Description |
|---|---|---|
| `gekka.actor.count` | ObservableGauge | Number of live actors by type |
| `gekka.actor.mailbox_size` | ObservableGauge | Pending messages in actor mailboxes |
| `gekka.sharding.shard_count` | ObservableGauge | Active shards per region per entity type |
| `gekka.sharding.entity_count` | ObservableGauge | Active entities per shard region |
| `gekka.persistence.recovery_duration` | Histogram | Time to replay events during actor recovery |
| `gekka.persistence.write_duration` | Histogram | Journal write latency per batch |
| `gekka.ddata.sync_duration` | Histogram | CRDT gossip round-trip latency |
| `gekka.ddata.delta_bytes` | Histogram | Gossip payload size (delta vs full-state) |

---

## Distributed Tracing

Gekka instruments the full **command → journal → projection** pipeline with
OpenTelemetry spans.  A single `TraceID` flows through all stages:

```
Command received
  └─ ShardRegion.Receive           (injects W3C TraceContext into ShardingEnvelope)
       └─ Journal.Write            (TracingJournal starts "Journal.Write" child span)
            └─ Projection.Handle  (projection runner starts "Projection.Handle" child span)
```

### Enabling Tracing

Configure the global OTel tracer provider before starting the actor system:

```go
exp, _ := jaeger.New(jaeger.WithCollectorEndpoint(
    jaeger.WithEndpoint("http://jaeger:14268/api/traces"),
))
tp := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(exp),
    sdktrace.WithResource(resource.NewWithAttributes(
        semconv.SchemaURL,
        semconv.ServiceName("my-service"),
    )),
)
otel.SetTracerProvider(tp)
otel.SetTextMapPropagator(propagation.TraceContext{})
```

Wrap the journal with `TracingJournal` to enable write/read spans:

```go
journal := persistence.NewTracingJournal(persistence.NewInMemoryJournal())
```

### Viewing Traces in Jaeger

1. Start Jaeger all-in-one: `docker run -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one`
2. Open `http://localhost:16686`
3. Select service `my-service` and search for operation `Projection.Handle`
4. Click any trace to see the full span tree from the originating command through the journal write to the projection handler

### Span Names

| Span Name | Instrumentation Scope | Description |
|---|---|---|
| `Journal.Write` | `github.com/sopranoworks/gekka/persistence` | One span per `AsyncWriteMessages` call |
| `Journal.Read` | `github.com/sopranoworks/gekka/persistence` | One span per `ReplayMessages` call |
| `Projection.Handle` | `github.com/sopranoworks/gekka/persistence/projection` | One span per event processed by a projection |

### W3C TraceContext Propagation

`TraceContext` is carried as a `map[string]string` of W3C headers
(`traceparent`, `tracestate`) at each pipeline boundary:

- `ShardingEnvelope.TraceContext` — injected by `ShardRegion` at message dispatch
- `PersistentRepr.TraceContext` — set by the persistent actor before writing
- `EventEnvelope.TraceContext` — copied from the journal into the read model

This allows projections to resume the exact trace that originated the command,
making it possible to observe the entire write-to-read latency in a single
waterfall view.

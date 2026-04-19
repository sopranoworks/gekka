# Operational Tooling Guide

Gekka provides two companion tools for cluster operators, maintained as
independent repositories:

## gekka-cli

Command-line interface for inspecting and managing a live cluster via the
HTTP Management API. Supports cluster membership, shard management,
interactive dashboard, and discovery diagnostics.

**Repository:** [github.com/sopranoworks/gekka-cli](https://github.com/sopranoworks/gekka-cli)

```bash
go install github.com/sopranoworks/gekka-cli@latest
```

## gekka-metrics

A full cluster node that joins the ring with the `metrics-exporter` role and
exports cluster metrics via the OpenTelemetry Protocol (OTLP/HTTP). Sees
membership changes in real time via gossip.

**Repository:** [github.com/sopranoworks/gekka-metrics](https://github.com/sopranoworks/gekka-metrics)

```bash
go install github.com/sopranoworks/gekka-metrics@latest
```

---

## Key Metrics Reference

### Currently Exported

| Metric | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `gekka.cluster.members` | ObservableGauge | `{members}` | `status`, `dc` | Number of cluster members grouped by status and data-center |

**Attribute values for `status`:** `up`, `joining`, `leaving`, `exiting`, `down`, `weakly-up`, `removed`

### Planned Metrics

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

See [DISTRIBUTED_TRACING.md](DISTRIBUTED_TRACING.md) for OpenTelemetry tracing
configuration, span names, and W3C TraceContext propagation.

# Cassandra Persistence Plugin — Design Spec

**Date:** 2026-04-11
**Phase:** 5 (Persistence plugin breadth) from `.plan/20260410.md`
**Gap:** G10 — Cassandra persistence plugin
**Approach:** Full Pekko schema compatibility (Approach A)

---

## 1. Scope

Implement a Cassandra persistence plugin under `extensions/persistence/cassandra/` that:

- Uses the **exact Pekko `akka-persistence-cassandra` table schema** so teams migrating from Pekko/Akka can read existing Cassandra data
- Implements all four gekka persistence interfaces: `Journal`, `SnapshotStore`, `DurableStateStore`, `ReadJournal`
- Uses `gocql/gocql` as the Cassandra driver
- Tests via `testcontainers-go` with a real Cassandra container behind the `integration` build tag
- Mirrors Pekko HOCON config namespace (`pekko.persistence.cassandra.*`)

LevelDB (G9) is **SKIPPED** — the Akka 2.6 multi-node compat tests contain zero persistence config.

---

## 2. Module Layout

```
extensions/persistence/cassandra/
  go.mod                  # module github.com/sopranoworks/gekka/extensions/persistence/cassandra
  register.go             # init() registration for all four providers
  config.go               # HOCON config parsing + validation
  journal.go              # CassandraJournal implements persistence.Journal
  snapshot.go             # CassandraSnapshotStore implements persistence.SnapshotStore
  state_store.go          # CassandraDurableStateStore implements persistence.DurableStateStore
  query_journal.go        # CassandraReadJournal (EventsByPersistenceId, EventsByTag)
  codec.go                # JSONCodec (reuse pattern from Redis/Spanner)
  schema.go               # CreateSchema() — keyspace + table DDL
  cassandra_test.go       # testcontainers-go integration tests
```

---

## 3. CQL Schema

### 3.1 Journal — `messages` table

```cql
CREATE TABLE IF NOT EXISTS <keyspace>.messages (
    persistence_id    text,
    partition_nr      bigint,
    sequence_nr       bigint,
    timestamp         timeuuid,
    writer_uuid       text,
    ser_id            int,
    ser_manifest      text,
    event_manifest    text,
    event             blob,
    tags              set<text>,
    meta_ser_id       int,
    meta_ser_manifest text,
    meta              blob,
    PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp)
) WITH CLUSTERING ORDER BY (sequence_nr ASC, timestamp ASC)
  AND gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

### 3.2 Partition metadata

```cql
CREATE TABLE IF NOT EXISTS <keyspace>.metadata (
    persistence_id text,
    partition_nr   bigint,
    PRIMARY KEY (persistence_id)
);
```

`target-partition-size` defaults to 500,000. When `sequence_nr > (partition_nr+1) * target_partition_size`, increment `partition_nr` in metadata and write to the new partition.

### 3.3 Snapshots

```cql
CREATE TABLE IF NOT EXISTS <snapshot_keyspace>.snapshots (
    persistence_id    text,
    sequence_nr       bigint,
    timestamp         bigint,
    ser_id            int,
    ser_manifest      text,
    snapshot_data     blob,
    meta_ser_id       int,
    meta_ser_manifest text,
    meta              blob,
    PRIMARY KEY (persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (sequence_nr DESC);
```

### 3.4 Tag views

```cql
CREATE TABLE IF NOT EXISTS <keyspace>.tag_views (
    tag_name       text,
    timebucket     bigint,
    timestamp      timeuuid,
    persistence_id text,
    sequence_nr    bigint,
    partition_nr   bigint,
    ser_id         int,
    ser_manifest   text,
    event_manifest text,
    event          blob,
    PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (timestamp ASC, persistence_id ASC, sequence_nr ASC);
```

Timebucket granularity: configurable (Day/Hour/Minute), default Day. Computed as `timestamp.UnixMilli() / bucketDurationMs`.

### 3.5 Tag scanning progress

```cql
CREATE TABLE IF NOT EXISTS <keyspace>.tag_scanning (
    persistence_id text,
    tag            text,
    sequence_nr    bigint,
    PRIMARY KEY (persistence_id, tag)
);
```

### 3.6 Durable state (gekka-native)

```cql
CREATE TABLE IF NOT EXISTS <keyspace>.durable_state (
    persistence_id text,
    revision       bigint,
    state_data     blob,
    state_manifest text,
    tag            text,
    PRIMARY KEY (persistence_id)
);
```

---

## 4. Registration & Config

### 4.1 Registration (`register.go`)

```go
func init() {
    persistence.RegisterJournalProvider("cassandra", newJournalFromConfig)
    persistence.RegisterSnapshotStoreProvider("cassandra", newSnapshotStoreFromConfig)
    persistence.RegisterDurableStateStoreProvider("cassandra", newDurableStateStoreFromConfig)
    persistence.RegisterReadJournalProvider("cassandra", newReadJournalFromConfig)
}
```

Each factory: validate config, create shared `*gocql.Session` (cached via `sync.Once` per contact-points+keyspace), instantiate store.

### 4.2 HOCON config

```hocon
pekko.persistence.cassandra {
  journal {
    keyspace = "pekko"
    table = "messages"
    metadata-table = "metadata"
    target-partition-size = 500000
    keyspace-autocreate = false
    tables-autocreate = false
    replication-strategy = "SimpleStrategy"
    replication-factor = 1
    gc-grace-seconds = 864000
  }
  snapshot {
    keyspace = "pekko_snapshot"
    table = "snapshots"
    keyspace-autocreate = false
    tables-autocreate = false
  }
  events-by-tag {
    table = "tag_views"
    scanning-table = "tag_scanning"
    bucket-size = "Day"
  }
  state {
    keyspace = "pekko"
    table = "durable_state"
  }
  contact-points = ["127.0.0.1"]
  port = 9042
  local-datacenter = "datacenter1"
  authentication {
    username = ""
    password = ""
  }
  connection {
    connect-timeout = "5s"
    read-timeout = "12s"
    num-connections = 2
  }
}
```

`ValidateConfig()` checks: `contact-points` non-empty, `port` > 0, `local-datacenter` non-empty.

### 4.3 Codec

Reuse the `PayloadCodec` interface / `JSONCodec` pattern from Redis/Spanner (copy, not cross-module import):

- `Encode(payload any) -> (manifest, data, error)`
- `Decode(manifest, data) -> (any, error)`
- `DefaultCodec` package-level var shared by all stores
- `Register(zero)` / `RegisterManifest(manifest, zero)` for type registration

---

## 5. Journal Implementation

### 5.1 Struct

```go
type CassandraJournal struct {
    session        *gocql.Session
    keyspace       string
    table          string         // "messages"
    metadataTable  string         // "metadata"
    targetPartSize int64          // 500000
    codec          PayloadCodec
    writerUUID     string         // uuid.New() at startup
    partCache      *lru.Cache     // persistenceId -> currentPartitionNr (sync-safe, 10K entries)
}
```

### 5.2 AsyncWriteMessages

1. For each message, resolve current `partition_nr`:
   - LRU cache hit -> use cached value
   - Cache miss -> query `metadata` table
   - If `seqNr > (partitionNr+1) * targetPartSize`, increment partition_nr, update `metadata`, update cache
2. Build `gocql.Batch` (UNLOGGED, within single partition — matches Pekko)
3. For tagged events, also INSERT into `tag_views` and UPDATE `tag_scanning` in the same batch
4. Execute batch

### 5.3 ReplayMessages

1. Read max `partition_nr` from `metadata`
2. Compute starting partition: `fromSeqNr / targetPartSize`
3. For each partition from start to max:
   ```cql
   SELECT sequence_nr, event, ser_manifest, event_manifest, ...
   FROM messages
   WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr >= ? AND sequence_nr <= ?
   ORDER BY sequence_nr ASC
   ```
4. Call `callback(repr)` per row, stop after `max` events
5. Decode payload via codec using stored manifest

### 5.4 ReadHighestSequenceNr

1. Read max partition_nr from `metadata`
2. Query last partition descending: `SELECT sequence_nr ... ORDER BY sequence_nr DESC LIMIT 1`
3. If empty and partition_nr > 0, walk backwards one partition (handles edge case of fully-deleted latest partition)

### 5.5 AsyncDeleteMessagesTo

Follow Pekko's approach:
1. Individual DELETE per row up to `toSeqNr` within each affected partition
2. Fully-consumed partitions become tombstoned naturally
3. Don't update `metadata` — partition_nr bookkeeping stays intact for replay

---

## 6. Snapshot Store

### 6.1 Struct

```go
type CassandraSnapshotStore struct {
    session  *gocql.Session
    keyspace string
    table    string        // "snapshots"
    codec    PayloadCodec
}
```

### 6.2 Methods

- **SaveSnapshot**: Encode via codec, INSERT with persistence_id, sequence_nr, timestamp (UnixNano), ser_manifest, snapshot_data
- **LoadSnapshot**: Query with `sequence_nr DESC`, filter by `SnapshotSelectionCriteria` (max/min seqNr in CQL, max/min timestamp post-filtered in Go)
- **DeleteSnapshot**: DELETE by `(persistence_id, sequence_nr)`
- **DeleteSnapshots**: SELECT matching rows by criteria, then batch DELETE (read-then-delete, same as Pekko)

---

## 7. Durable State Store

### 7.1 Struct

```go
type CassandraDurableStateStore struct {
    session  *gocql.Session
    keyspace string
    table    string        // "durable_state"
    codec    PayloadCodec
}
```

### 7.2 Methods

- **Get**: SELECT by persistence_id (single-row partition), decode with codec, return `(state, revision, err)`
- **Upsert**: Encode via codec, INSERT (Cassandra INSERT = natural upsert)
- **Delete**: DELETE by persistence_id

Single-partition operations, no bucketing complexity.

---

## 8. Query Journal (Read-Side)

### 8.1 Struct

```go
type CassandraReadJournal struct {
    session        *gocql.Session
    keyspace       string
    journalTable   string        // "messages"
    metadataTable  string        // "metadata"
    tagTable       string        // "tag_views"
    scanningTable  string        // "tag_scanning"
    targetPartSize int64
    bucketSize     BucketSize    // Day | Hour | Minute
    codec          PayloadCodec
}
```

### 8.2 EventsByPersistenceId (live stream)

- Returns channel-based `EventSource` streaming `EventEnvelope` values
- Walks partitions same as ReplayMessages but unbounded
- Polls latest partition every `refresh-interval` (default 3s) for new events
- Closes on context cancellation

### 8.3 CurrentEventsByPersistenceId (finite stream)

- Same partition walk but completes after reaching current highest sequence_nr (no polling)

### 8.4 EventsByTag (live stream)

- Scans `tag_views` starting from a `TimeBasedOffset` (wraps timeuuid)
- Iterates timebuckets forward:
  ```cql
  SELECT * FROM tag_views
  WHERE tag_name = ? AND timebucket = ? AND timestamp > ?
  ORDER BY timestamp ASC
  ```
- Advances to next timebucket when current is exhausted
- Polls current bucket on `refresh-interval` for live-tail
- Returns `EventEnvelope` with `TimeBasedOffset` for position tracking

### 8.5 CurrentEventsByTag (finite stream)

- Same scan but completes when reaching "now" (no live-tail polling)

### 8.6 Offset type

```go
type TimeBasedOffset struct {
    Timestamp time.Time
    TagName   string
}
```

Compatible with the existing `persistence.Offset` interface used by the projection framework.

### 8.7 Timebucket computation

```go
func timebucket(t time.Time, size BucketSize) int64 {
    switch size {
    case Day:    return t.UnixMilli() / 86_400_000
    case Hour:   return t.UnixMilli() / 3_600_000
    case Minute: return t.UnixMilli() / 60_000
    }
}
```

---

## 9. Testing

### 9.1 Container setup

- Image: `cassandra:4.1`
- Wait: `wait.ForLog("Starting listening for CQL clients")` + `wait.ForListeningPort("9042/tcp")`
- Build tag: `//go:build integration`
- `TestMain(m *testing.M)` starts container once, stores `testContactPoint`

### 9.2 Test isolation

Each test function creates a unique keyspace (`test_<randomhex>`) via `CreateSchema()`.

### 9.3 Test cases

**Journal:**
- Write N events -> ReplayMessages -> verify payload order and content
- ReadHighestSequenceNr after writes
- Partition rollover: write `targetPartSize + 1` events (use small targetPartSize=10) -> replay spans two partitions
- AsyncDeleteMessagesTo -> replay skips deleted range
- Tagged writes -> verify tag_views populated

**Snapshots:**
- Save -> Load with criteria (max/min seqNr, max/min timestamp)
- Delete single -> Load returns next best
- DeleteSnapshots with criteria -> batch removal

**DurableState:**
- Upsert -> Get -> verify state + revision
- Upsert overwrite -> Get returns latest
- Delete -> Get returns not-found

**Query journal:**
- CurrentEventsByPersistenceId: write 5 events -> stream completes with 5
- CurrentEventsByTag: write tagged events -> stream returns only matching tag
- EventsByTag timebucket boundary: events spanning two buckets -> both returned in order

### 9.4 Schema bootstrap

`CreateSchema(session, config)` executes DDL only when `keyspace-autocreate` / `tables-autocreate` are true. Production users run DDL themselves; autocreate is dev/test convenience (default `false`, same as Pekko).

---

## 10. LevelDB Decision

G9 is **SKIPPED**. The Akka 2.6 multi-node compat tests (`test/compatibility/akka-multi-node/`) contain:
- Zero `akka.persistence` config sections
- No LevelDB references
- Only cluster membership, Artery transport, and echo-actor message exchange tests

Checkbox in `.work/20260410.md` marked: `[~] SKIPPED: not required by test/compatibility/akka-multi-node`.

---

## 11. Dependencies

- `github.com/gocql/gocql` — Cassandra driver
- `github.com/google/uuid` — writer UUID generation (likely already in go.mod)
- `github.com/hashicorp/golang-lru/v2` — partition_nr LRU cache (10K entries, typed generic API)
- `github.com/testcontainers/testcontainers-go` — integration test container (test-only)

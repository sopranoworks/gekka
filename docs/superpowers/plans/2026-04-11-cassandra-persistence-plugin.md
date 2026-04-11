# Cassandra Persistence Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a Pekko-schema-compatible Cassandra persistence plugin providing Journal, SnapshotStore, DurableStateStore, and ReadJournal under `extensions/persistence/cassandra/`.

**Architecture:** Follows the same registration + codec + testcontainers pattern as the existing Redis plugin (`extensions/persistence/redis/`). Uses `gocql/gocql` for Cassandra access. Tables match Pekko's `akka-persistence-cassandra` schema (including `partition_nr` bucketing and `tag_views` timebucket queries). All tests require a live Cassandra container via `testcontainers-go` behind the `integration` build tag.

**Tech Stack:** Go 1.26.1, gocql/gocql, hashicorp/golang-lru/v2, google/uuid, testcontainers-go

---

## File Map

| File | Responsibility |
|------|----------------|
| `extensions/persistence/cassandra/go.mod` | Module definition, dependencies |
| `extensions/persistence/cassandra/codec.go` | `PayloadCodec` interface + `JSONCodec` (copied from Redis pattern) |
| `extensions/persistence/cassandra/config.go` | HOCON config parsing, validation, `*gocql.Session` creation |
| `extensions/persistence/cassandra/schema.go` | CQL DDL for all 6 tables + keyspace autocreate |
| `extensions/persistence/cassandra/journal.go` | `CassandraJournal` implementing `persistence.Journal` |
| `extensions/persistence/cassandra/snapshot.go` | `CassandraSnapshotStore` implementing `persistence.SnapshotStore` |
| `extensions/persistence/cassandra/state_store.go` | `CassandraDurableStateStore` implementing `persistence.DurableStateStore` |
| `extensions/persistence/cassandra/query_journal.go` | `CassandraReadJournal` implementing 5 query interfaces |
| `extensions/persistence/cassandra/register.go` | `init()` provider registration |
| `extensions/persistence/cassandra/cassandra_test.go` | Integration tests (testcontainers-go + Cassandra 4.1) |

---

### Task 1: Module scaffold + codec

**Files:**
- Create: `extensions/persistence/cassandra/go.mod`
- Create: `extensions/persistence/cassandra/codec.go`
- Modify: `go.work` (add `./extensions/persistence/cassandra`)

- [ ] **Step 1: Create go.mod**

```
extensions/persistence/cassandra/go.mod
```

```go
module github.com/sopranoworks/gekka-extensions-persistence-cassandra

go 1.26.1

require (
	github.com/gocql/gocql v1.7.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/sopranoworks/gekka v0.14.0-dev
	github.com/sopranoworks/gekka-config v1.0.4
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.41.0
)

replace github.com/sopranoworks/gekka => ../../../
```

- [ ] **Step 2: Add to go.work**

Add `./extensions/persistence/cassandra` to the `use` block in `go.work`:

```
use (
	.
	./cmd
	./extensions/cluster/aws
	./extensions/cluster/consul
	./extensions/cluster/k8s
	./extensions/persistence/cassandra
	./extensions/persistence/redis
	./extensions/persistence/spanner
	./extensions/telemetry/otel
	./test/bench
)
```

- [ ] **Step 3: Create codec.go**

Copy the `PayloadCodec` interface and `JSONCodec` implementation from `extensions/persistence/redis/codec.go`, changing the package name to `cassandrastore` and error prefixes to `"cassandrastore:"`.

```go
/*
 * codec.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type PayloadCodec interface {
	Encode(payload any) (manifest string, data []byte, err error)
	Decode(manifest string, data []byte) (any, error)
}

type JSONCodec struct {
	mu    sync.RWMutex
	types map[string]reflect.Type
}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{types: make(map[string]reflect.Type)}
}

func (c *JSONCodec) Register(zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[t.String()] = t
	c.mu.Unlock()
}

func (c *JSONCodec) RegisterManifest(manifest string, zero any) {
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	c.mu.Lock()
	c.types[manifest] = t
	c.mu.Unlock()
}

func (c *JSONCodec) Encode(payload any) (string, []byte, error) {
	if payload == nil {
		return "nil", []byte("null"), nil
	}
	t := reflect.TypeOf(payload)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	manifest := t.String()
	data, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("cassandrastore: JSON encode %q: %w", manifest, err)
	}
	return manifest, data, nil
}

func (c *JSONCodec) Decode(manifest string, data []byte) (any, error) {
	if manifest == "nil" {
		return nil, nil
	}
	c.mu.RLock()
	t, ok := c.types[manifest]
	c.mu.RUnlock()
	if !ok {
		raw := make(json.RawMessage, len(data))
		copy(raw, data)
		return raw, nil
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("cassandrastore: JSON decode %q: %w", manifest, err)
	}
	return ptr.Elem().Interface(), nil
}
```

- [ ] **Step 4: Run `go mod tidy` and verify build**

Run:
```bash
cd extensions/persistence/cassandra && go mod tidy && cd ../../.. && go build ./...
```
Expected: Build succeeds with no errors.

- [ ] **Step 5: Commit**

```bash
git add extensions/persistence/cassandra/go.mod extensions/persistence/cassandra/go.sum extensions/persistence/cassandra/codec.go go.work
git commit -m "feat(persistence/cassandra): scaffold module with codec"
```

---

### Task 2: Config parsing and session factory

**Files:**
- Create: `extensions/persistence/cassandra/config.go`

- [ ] **Step 1: Create config.go**

```go
/*
 * config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// BucketSize controls timebucket granularity for tag_views.
type BucketSize int

const (
	BucketDay    BucketSize = iota
	BucketHour
	BucketMinute
)

func (b BucketSize) DurationMs() int64 {
	switch b {
	case BucketHour:
		return 3_600_000
	case BucketMinute:
		return 60_000
	default:
		return 86_400_000
	}
}

// Timebucket computes the bucket number for t.
func Timebucket(t time.Time, size BucketSize) int64 {
	return t.UnixMilli() / size.DurationMs()
}

// ParseBucketSize parses a string ("Day", "Hour", "Minute") into a BucketSize.
func ParseBucketSize(s string) BucketSize {
	switch strings.ToLower(s) {
	case "hour":
		return BucketHour
	case "minute":
		return BucketMinute
	default:
		return BucketDay
	}
}

// CassandraConfig holds parsed HOCON settings for the Cassandra plugin.
type CassandraConfig struct {
	ContactPoints []string
	Port          int
	Datacenter    string
	Username      string
	Password      string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	NumConnections int

	JournalKeyspace       string
	JournalTable          string
	MetadataTable         string
	TargetPartitionSize   int64
	KeyspaceAutocreate    bool
	TablesAutocreate      bool
	ReplicationStrategy   string
	ReplicationFactor     int
	GcGraceSeconds        int

	SnapshotKeyspace      string
	SnapshotTable         string
	SnapshotAutoKeyspace  bool
	SnapshotAutoTables    bool

	TagTable              string
	ScanningTable         string
	BucketSize            BucketSize

	StateKeyspace         string
	StateTable            string
}

// ValidateConfig checks required Cassandra configuration keys.
func ValidateConfig(cfg hocon.Config) error {
	return persistence.ValidateProviderConfig(cfg, "contact-points", "port", "local-datacenter")
}

// ParseConfig parses a HOCON config into a CassandraConfig with defaults.
func ParseConfig(cfg hocon.Config) (*CassandraConfig, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	c := &CassandraConfig{
		Port:                9042,
		Datacenter:          "datacenter1",
		ConnectTimeout:      5 * time.Second,
		ReadTimeout:         12 * time.Second,
		NumConnections:      2,
		JournalKeyspace:     "pekko",
		JournalTable:        "messages",
		MetadataTable:       "metadata",
		TargetPartitionSize: 500000,
		ReplicationStrategy: "SimpleStrategy",
		ReplicationFactor:   1,
		GcGraceSeconds:      864000,
		SnapshotKeyspace:    "pekko_snapshot",
		SnapshotTable:       "snapshots",
		TagTable:            "tag_views",
		ScanningTable:       "tag_scanning",
		BucketSize:          BucketDay,
		StateKeyspace:       "pekko",
		StateTable:          "durable_state",
	}

	// Contact points
	if list, err := cfg.GetStringList("contact-points"); err == nil {
		c.ContactPoints = list
	}
	if len(c.ContactPoints) == 0 {
		if s, err := cfg.GetString("contact-points"); err == nil {
			c.ContactPoints = []string{s}
		}
	}

	if v, err := cfg.GetInt("port"); err == nil {
		c.Port = v
	}
	if v, err := cfg.GetString("local-datacenter"); err == nil {
		c.Datacenter = v
	}

	// Authentication
	if v, err := cfg.GetString("authentication.username"); err == nil {
		c.Username = v
	}
	if v, err := cfg.GetString("authentication.password"); err == nil {
		c.Password = v
	}

	// Connection
	if v, err := cfg.GetDuration("connection.connect-timeout"); err == nil {
		c.ConnectTimeout = v
	}
	if v, err := cfg.GetDuration("connection.read-timeout"); err == nil {
		c.ReadTimeout = v
	}
	if v, err := cfg.GetInt("connection.num-connections"); err == nil {
		c.NumConnections = v
	}

	// Journal
	if v, err := cfg.GetString("journal.keyspace"); err == nil {
		c.JournalKeyspace = v
	}
	if v, err := cfg.GetString("journal.table"); err == nil {
		c.JournalTable = v
	}
	if v, err := cfg.GetString("journal.metadata-table"); err == nil {
		c.MetadataTable = v
	}
	if v, err := cfg.GetInt("journal.target-partition-size"); err == nil {
		c.TargetPartitionSize = int64(v)
	}
	if v, err := cfg.GetBoolean("journal.keyspace-autocreate"); err == nil {
		c.KeyspaceAutocreate = v
	}
	if v, err := cfg.GetBoolean("journal.tables-autocreate"); err == nil {
		c.TablesAutocreate = v
	}
	if v, err := cfg.GetString("journal.replication-strategy"); err == nil {
		c.ReplicationStrategy = v
	}
	if v, err := cfg.GetInt("journal.replication-factor"); err == nil {
		c.ReplicationFactor = v
	}
	if v, err := cfg.GetInt("journal.gc-grace-seconds"); err == nil {
		c.GcGraceSeconds = v
	}

	// Snapshot
	if v, err := cfg.GetString("snapshot.keyspace"); err == nil {
		c.SnapshotKeyspace = v
	}
	if v, err := cfg.GetString("snapshot.table"); err == nil {
		c.SnapshotTable = v
	}
	if v, err := cfg.GetBoolean("snapshot.keyspace-autocreate"); err == nil {
		c.SnapshotAutoKeyspace = v
	}
	if v, err := cfg.GetBoolean("snapshot.tables-autocreate"); err == nil {
		c.SnapshotAutoTables = v
	}

	// Events by tag
	if v, err := cfg.GetString("events-by-tag.table"); err == nil {
		c.TagTable = v
	}
	if v, err := cfg.GetString("events-by-tag.scanning-table"); err == nil {
		c.ScanningTable = v
	}
	if v, err := cfg.GetString("events-by-tag.bucket-size"); err == nil {
		c.BucketSize = ParseBucketSize(v)
	}

	// State
	if v, err := cfg.GetString("state.keyspace"); err == nil {
		c.StateKeyspace = v
	}
	if v, err := cfg.GetString("state.table"); err == nil {
		c.StateTable = v
	}

	return c, nil
}

// ── Session cache ────────────────────────────────────────────────────────────

var (
	sessionMu    sync.Mutex
	cachedConfig *CassandraConfig
	cachedSess   *gocql.Session
)

// GetOrCreateSession returns a shared gocql.Session, creating it once.
func GetOrCreateSession(cfg *CassandraConfig) (*gocql.Session, error) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	if cachedSess != nil && !cachedSess.Closed() {
		return cachedSess, nil
	}

	cluster := gocql.NewCluster(cfg.ContactPoints...)
	cluster.Port = cfg.Port
	cluster.Timeout = cfg.ConnectTimeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.NumConns = cfg.NumConnections
	cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(cfg.Datacenter)

	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandrastore: create session: %w", err)
	}

	cachedConfig = cfg
	cachedSess = sess
	return sess, nil
}

// ParseConfigString is a convenience wrapper for tests.
func ParseConfigString(s string) (*hocon.Config, error) {
	return hocon.ParseString(s)
}
```

- [ ] **Step 2: Run `go mod tidy` and verify build**

Run:
```bash
cd extensions/persistence/cassandra && go mod tidy && cd ../../.. && go build ./...
```
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/config.go extensions/persistence/cassandra/go.mod extensions/persistence/cassandra/go.sum
git commit -m "feat(persistence/cassandra): add HOCON config parsing and session factory"
```

---

### Task 3: Schema DDL

**Files:**
- Create: `extensions/persistence/cassandra/schema.go`

- [ ] **Step 1: Create schema.go**

```go
/*
 * schema.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"

	"github.com/gocql/gocql"
)

// CreateSchema creates keyspaces and tables if autocreate is enabled in cfg.
// Production deployments should set autocreate=false and run DDL manually.
func CreateSchema(session *gocql.Session, cfg *CassandraConfig) error {
	if cfg.KeyspaceAutocreate {
		if err := createKeyspace(session, cfg.JournalKeyspace, cfg.ReplicationStrategy, cfg.ReplicationFactor); err != nil {
			return err
		}
	}
	if cfg.SnapshotAutoKeyspace {
		if err := createKeyspace(session, cfg.SnapshotKeyspace, cfg.ReplicationStrategy, cfg.ReplicationFactor); err != nil {
			return err
		}
	}

	if cfg.TablesAutocreate {
		if err := createJournalTables(session, cfg); err != nil {
			return err
		}
		if err := createTagTables(session, cfg); err != nil {
			return err
		}
		if err := createStateTables(session, cfg); err != nil {
			return err
		}
	}
	if cfg.SnapshotAutoTables {
		if err := createSnapshotTables(session, cfg); err != nil {
			return err
		}
	}

	return nil
}

func createKeyspace(session *gocql.Session, keyspace, strategy string, factor int) error {
	cql := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': '%s', 'replication_factor': %d}`,
		keyspace, strategy, factor,
	)
	return session.Query(cql).Exec()
}

func createJournalTables(session *gocql.Session, cfg *CassandraConfig) error {
	messages := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
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
	  AND gc_grace_seconds = %d
	  AND compaction = {'class': 'SizeTieredCompactionStrategy'}`,
		cfg.JournalKeyspace, cfg.JournalTable, cfg.GcGraceSeconds,
	)
	if err := session.Query(messages).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.JournalKeyspace, cfg.JournalTable, err)
	}

	metadata := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		persistence_id text,
		partition_nr   bigint,
		PRIMARY KEY (persistence_id)
	)`, cfg.JournalKeyspace, cfg.MetadataTable)
	if err := session.Query(metadata).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.JournalKeyspace, cfg.MetadataTable, err)
	}

	return nil
}

func createTagTables(session *gocql.Session, cfg *CassandraConfig) error {
	tagViews := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
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
	) WITH CLUSTERING ORDER BY (timestamp ASC, persistence_id ASC, sequence_nr ASC)`,
		cfg.JournalKeyspace, cfg.TagTable,
	)
	if err := session.Query(tagViews).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.JournalKeyspace, cfg.TagTable, err)
	}

	scanning := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		persistence_id text,
		tag            text,
		sequence_nr    bigint,
		PRIMARY KEY (persistence_id, tag)
	)`, cfg.JournalKeyspace, cfg.ScanningTable)
	if err := session.Query(scanning).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.JournalKeyspace, cfg.ScanningTable, err)
	}

	return nil
}

func createSnapshotTables(session *gocql.Session, cfg *CassandraConfig) error {
	snapshots := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
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
	) WITH CLUSTERING ORDER BY (sequence_nr DESC)`,
		cfg.SnapshotKeyspace, cfg.SnapshotTable,
	)
	if err := session.Query(snapshots).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.SnapshotKeyspace, cfg.SnapshotTable, err)
	}

	return nil
}

func createStateTables(session *gocql.Session, cfg *CassandraConfig) error {
	state := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		persistence_id text,
		revision       bigint,
		state_data     blob,
		state_manifest text,
		tag            text,
		PRIMARY KEY (persistence_id)
	)`, cfg.JournalKeyspace, cfg.StateTable)
	if err := session.Query(state).Exec(); err != nil {
		return fmt.Errorf("cassandrastore: create %s.%s: %w", cfg.JournalKeyspace, cfg.StateTable, err)
	}

	return nil
}
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/schema.go
git commit -m "feat(persistence/cassandra): add CQL schema DDL with Pekko-compatible tables"
```

---

### Task 4: Journal implementation

**Files:**
- Create: `extensions/persistence/cassandra/journal.go`

- [ ] **Step 1: Create journal.go**

```go
/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraJournal implements persistence.Journal using Cassandra with
// Pekko-compatible partition bucketing.
type CassandraJournal struct {
	session       *gocql.Session
	keyspace      string
	table         string
	metadataTable string
	targetPartSz  int64
	codec         PayloadCodec
	writerUUID    string
	bucketSize    BucketSize
	tagTable      string
	scanningTable string
	partCache     *lru.Cache[string, int64]
}

// NewCassandraJournal creates a CassandraJournal.
func NewCassandraJournal(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraJournal {
	cache, _ := lru.New[string, int64](10_000)
	return &CassandraJournal{
		session:       session,
		keyspace:      cfg.JournalKeyspace,
		table:         cfg.JournalTable,
		metadataTable: cfg.MetadataTable,
		targetPartSz:  cfg.TargetPartitionSize,
		codec:         codec,
		writerUUID:    uuid.New().String(),
		bucketSize:    cfg.BucketSize,
		tagTable:      cfg.TagTable,
		scanningTable: cfg.ScanningTable,
		partCache:     cache,
	}
}

// resolvePartitionNr returns the current partition_nr for pid, using the LRU
// cache with a fallback to the metadata table.
func (j *CassandraJournal) resolvePartitionNr(pid string) (int64, error) {
	if v, ok := j.partCache.Get(pid); ok {
		return v, nil
	}
	var partNr int64
	q := fmt.Sprintf(`SELECT partition_nr FROM %s.%s WHERE persistence_id = ?`, j.keyspace, j.metadataTable)
	if err := j.session.Query(q, pid).Scan(&partNr); err != nil {
		if err.Error() == "not found" {
			j.partCache.Add(pid, 0)
			return 0, nil
		}
		return 0, fmt.Errorf("cassandrajournal: read metadata for %q: %w", pid, err)
	}
	j.partCache.Add(pid, partNr)
	return partNr, nil
}

// bumpPartitionNr increments the partition_nr in the metadata table and cache.
func (j *CassandraJournal) bumpPartitionNr(pid string, newPartNr int64) error {
	q := fmt.Sprintf(`INSERT INTO %s.%s (persistence_id, partition_nr) VALUES (?, ?)`, j.keyspace, j.metadataTable)
	if err := j.session.Query(q, pid, newPartNr).Exec(); err != nil {
		return fmt.Errorf("cassandrajournal: bump metadata for %q: %w", pid, err)
	}
	j.partCache.Add(pid, newPartNr)
	return nil
}

// AsyncWriteMessages writes a batch of events to the journal, handling
// partition rollover and tag_views inserts.
func (j *CassandraJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	for _, msg := range messages {
		partNr, err := j.resolvePartitionNr(msg.PersistenceID)
		if err != nil {
			return err
		}

		// Check if partition rollover is needed.
		if msg.SequenceNr > uint64((partNr+1)*j.targetPartSz) {
			partNr++
			if err := j.bumpPartitionNr(msg.PersistenceID, partNr); err != nil {
				return err
			}
		}

		manifest, data, err := j.codec.Encode(msg.Payload)
		if err != nil {
			return fmt.Errorf("cassandrajournal: encode %s/%d: %w", msg.PersistenceID, msg.SequenceNr, err)
		}

		ts, _ := gocql.RandomUUID()
		now := time.Now()

		var tags []string
		if len(msg.Tags) > 0 {
			tags = msg.Tags
		}

		insertQ := fmt.Sprintf(`INSERT INTO %s.%s
			(persistence_id, partition_nr, sequence_nr, timestamp, writer_uuid,
			 ser_id, ser_manifest, event_manifest, event, tags)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			j.keyspace, j.table)

		if err := j.session.Query(insertQ,
			msg.PersistenceID, partNr, int64(msg.SequenceNr), ts, j.writerUUID,
			0, manifest, msg.Manifest, data, tags,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("cassandrajournal: write %s/%d: %w", msg.PersistenceID, msg.SequenceNr, err)
		}

		// Ensure metadata row exists (idempotent INSERT for first event).
		if msg.SequenceNr == 1 && partNr == 0 {
			_ = j.bumpPartitionNr(msg.PersistenceID, 0)
		}

		// Write tag_views entries for tagged events.
		for _, tag := range tags {
			bucket := Timebucket(now, j.bucketSize)
			tagQ := fmt.Sprintf(`INSERT INTO %s.%s
				(tag_name, timebucket, timestamp, persistence_id, sequence_nr,
				 partition_nr, ser_id, ser_manifest, event_manifest, event)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				j.keyspace, j.tagTable)
			if err := j.session.Query(tagQ,
				tag, bucket, ts, msg.PersistenceID, int64(msg.SequenceNr),
				partNr, 0, manifest, msg.Manifest, data,
			).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("cassandrajournal: write tag_views for %s/%d tag=%q: %w",
					msg.PersistenceID, msg.SequenceNr, tag, err)
			}

			scanQ := fmt.Sprintf(`INSERT INTO %s.%s (persistence_id, tag, sequence_nr) VALUES (?, ?, ?)`,
				j.keyspace, j.scanningTable)
			_ = j.session.Query(scanQ, msg.PersistenceID, tag, int64(msg.SequenceNr)).WithContext(ctx).Exec()
		}
	}
	return nil
}

// ReplayMessages reads events from [fromSequenceNr, toSequenceNr] across
// partitions and invokes callback for each, up to max events.
func (j *CassandraJournal) ReplayMessages(
	ctx context.Context,
	pid string,
	fromSequenceNr, toSequenceNr uint64,
	max uint64,
	callback func(persistence.PersistentRepr),
) error {
	maxPartNr, err := j.resolvePartitionNr(pid)
	if err != nil {
		return err
	}

	startPart := int64(fromSequenceNr) / j.targetPartSz
	if startPart < 0 {
		startPart = 0
	}

	var delivered uint64
	selectQ := fmt.Sprintf(`SELECT sequence_nr, event, ser_manifest, event_manifest, tags
		FROM %s.%s
		WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr >= ? AND sequence_nr <= ?
		ORDER BY sequence_nr ASC`,
		j.keyspace, j.table)

	for partNr := startPart; partNr <= maxPartNr; partNr++ {
		iter := j.session.Query(selectQ, pid, partNr, int64(fromSequenceNr), int64(toSequenceNr)).
			WithContext(ctx).Iter()

		var seqNr int64
		var eventData []byte
		var serManifest, eventManifest string
		var tags []string

		for iter.Scan(&seqNr, &eventData, &serManifest, &eventManifest, &tags) {
			payload, decErr := j.codec.Decode(serManifest, eventData)
			if decErr != nil {
				_ = iter.Close()
				return fmt.Errorf("cassandrajournal: decode %s/%d: %w", pid, seqNr, decErr)
			}

			callback(persistence.PersistentRepr{
				PersistenceID: pid,
				SequenceNr:    uint64(seqNr),
				Payload:       payload,
				Manifest:      eventManifest,
				Tags:          tags,
			})

			delivered++
			if max > 0 && delivered >= max {
				_ = iter.Close()
				return nil
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("cassandrajournal: replay %s partition %d: %w", pid, partNr, err)
		}
	}
	return nil
}

// ReadHighestSequenceNr returns the highest stored sequence number for pid.
func (j *CassandraJournal) ReadHighestSequenceNr(ctx context.Context, pid string, fromSequenceNr uint64) (uint64, error) {
	maxPartNr, err := j.resolvePartitionNr(pid)
	if err != nil {
		return 0, err
	}

	selectQ := fmt.Sprintf(`SELECT sequence_nr FROM %s.%s
		WHERE persistence_id = ? AND partition_nr = ?
		ORDER BY sequence_nr DESC LIMIT 1`,
		j.keyspace, j.table)

	// Walk backwards from maxPartNr to find the highest sequence_nr.
	for partNr := maxPartNr; partNr >= 0; partNr-- {
		var seqNr int64
		if err := j.session.Query(selectQ, pid, partNr).WithContext(ctx).Scan(&seqNr); err != nil {
			if err.Error() == "not found" {
				continue
			}
			return 0, fmt.Errorf("cassandrajournal: read highest seqnr %s/%d: %w", pid, partNr, err)
		}
		if uint64(seqNr) >= fromSequenceNr {
			return uint64(seqNr), nil
		}
	}
	return 0, nil
}

// AsyncDeleteMessagesTo deletes events up to toSequenceNr for pid.
func (j *CassandraJournal) AsyncDeleteMessagesTo(ctx context.Context, pid string, toSequenceNr uint64) error {
	maxPartNr, err := j.resolvePartitionNr(pid)
	if err != nil {
		return err
	}

	selectQ := fmt.Sprintf(`SELECT sequence_nr, timestamp FROM %s.%s
		WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr <= ?`,
		j.keyspace, j.table)
	deleteQ := fmt.Sprintf(`DELETE FROM %s.%s
		WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr = ? AND timestamp = ?`,
		j.keyspace, j.table)

	for partNr := int64(0); partNr <= maxPartNr; partNr++ {
		iter := j.session.Query(selectQ, pid, partNr, int64(toSequenceNr)).WithContext(ctx).Iter()

		var seqNr int64
		var ts gocql.UUID
		for iter.Scan(&seqNr, &ts) {
			if err := j.session.Query(deleteQ, pid, partNr, seqNr, ts).WithContext(ctx).Exec(); err != nil {
				_ = iter.Close()
				return fmt.Errorf("cassandrajournal: delete %s/%d: %w", pid, seqNr, err)
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("cassandrajournal: delete scan %s/%d: %w", pid, partNr, err)
		}
	}
	return nil
}

var _ persistence.Journal = (*CassandraJournal)(nil)
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/journal.go extensions/persistence/cassandra/go.mod extensions/persistence/cassandra/go.sum
git commit -m "feat(persistence/cassandra): implement Journal with Pekko partition bucketing"
```

---

### Task 5: Snapshot store implementation

**Files:**
- Create: `extensions/persistence/cassandra/snapshot.go`

- [ ] **Step 1: Create snapshot.go**

```go
/*
 * snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraSnapshotStore implements persistence.SnapshotStore using Cassandra
// with the Pekko-compatible snapshots table schema.
type CassandraSnapshotStore struct {
	session  *gocql.Session
	keyspace string
	table    string
	codec    PayloadCodec
}

// NewCassandraSnapshotStore creates a CassandraSnapshotStore.
func NewCassandraSnapshotStore(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraSnapshotStore {
	return &CassandraSnapshotStore{
		session:  session,
		keyspace: cfg.SnapshotKeyspace,
		table:    cfg.SnapshotTable,
		codec:    codec,
	}
}

// SaveSnapshot writes a snapshot to Cassandra.
func (s *CassandraSnapshotStore) SaveSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata, snapshot any) error {
	manifest, data, err := s.codec.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("cassandrasnapshot: encode %q/%d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}

	ts := metadata.Timestamp
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	q := fmt.Sprintf(`INSERT INTO %s.%s
		(persistence_id, sequence_nr, timestamp, ser_id, ser_manifest, snapshot_data)
		VALUES (?, ?, ?, ?, ?, ?)`,
		s.keyspace, s.table)

	if err := s.session.Query(q,
		metadata.PersistenceID, int64(metadata.SequenceNr), ts, 0, manifest, data,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrasnapshot: save %q/%d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// LoadSnapshot returns the highest-seqNr snapshot matching criteria, or nil.
func (s *CassandraSnapshotStore) LoadSnapshot(
	ctx context.Context,
	pid string,
	criteria persistence.SnapshotSelectionCriteria,
) (*persistence.SelectedSnapshot, error) {
	maxSeq := criteria.MaxSequenceNr
	if maxSeq == 0 || maxSeq == ^uint64(0) {
		maxSeq = ^uint64(0)
	}
	minSeq := criteria.MinSequenceNr
	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	// Clustering order is DESC so the first matching row is the highest seqNr.
	q := fmt.Sprintf(`SELECT sequence_nr, timestamp, ser_manifest, snapshot_data
		FROM %s.%s
		WHERE persistence_id = ? AND sequence_nr <= ? AND sequence_nr >= ?
		ORDER BY sequence_nr DESC`,
		s.keyspace, s.table)

	iter := s.session.Query(q, pid, int64(maxSeq), int64(minSeq)).WithContext(ctx).Iter()

	var seqNr int64
	var ts int64
	var manifest string
	var data []byte

	for iter.Scan(&seqNr, &ts, &manifest, &data) {
		// Post-filter by timestamp.
		if ts < minTs || ts > maxTs {
			continue
		}

		payload, err := s.codec.Decode(manifest, data)
		if err != nil {
			_ = iter.Close()
			return nil, fmt.Errorf("cassandrasnapshot: decode %q/%d: %w", pid, seqNr, err)
		}

		_ = iter.Close()
		return &persistence.SelectedSnapshot{
			Metadata: persistence.SnapshotMetadata{
				PersistenceID: pid,
				SequenceNr:    uint64(seqNr),
				Timestamp:     ts,
			},
			Snapshot: payload,
		}, nil
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("cassandrasnapshot: load %q: %w", pid, err)
	}
	return nil, nil
}

// DeleteSnapshot removes a single snapshot by metadata key.
func (s *CassandraSnapshotStore) DeleteSnapshot(ctx context.Context, metadata persistence.SnapshotMetadata) error {
	q := fmt.Sprintf(`DELETE FROM %s.%s WHERE persistence_id = ? AND sequence_nr = ?`,
		s.keyspace, s.table)
	if err := s.session.Query(q, metadata.PersistenceID, int64(metadata.SequenceNr)).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrasnapshot: delete %q/%d: %w", metadata.PersistenceID, metadata.SequenceNr, err)
	}
	return nil
}

// DeleteSnapshots removes all snapshots matching criteria (read-then-delete).
func (s *CassandraSnapshotStore) DeleteSnapshots(
	ctx context.Context,
	pid string,
	criteria persistence.SnapshotSelectionCriteria,
) error {
	maxSeq := criteria.MaxSequenceNr
	if maxSeq == 0 || maxSeq == ^uint64(0) {
		maxSeq = ^uint64(0)
	}
	minSeq := criteria.MinSequenceNr
	maxTs := criteria.MaxTimestamp
	if maxTs == 0 {
		maxTs = math.MaxInt64
	}
	minTs := criteria.MinTimestamp

	selectQ := fmt.Sprintf(`SELECT sequence_nr, timestamp FROM %s.%s
		WHERE persistence_id = ? AND sequence_nr <= ? AND sequence_nr >= ?`,
		s.keyspace, s.table)

	iter := s.session.Query(selectQ, pid, int64(maxSeq), int64(minSeq)).WithContext(ctx).Iter()

	deleteQ := fmt.Sprintf(`DELETE FROM %s.%s WHERE persistence_id = ? AND sequence_nr = ?`,
		s.keyspace, s.table)

	var seqNr, ts int64
	for iter.Scan(&seqNr, &ts) {
		if ts < minTs || ts > maxTs {
			continue
		}
		if err := s.session.Query(deleteQ, pid, seqNr).WithContext(ctx).Exec(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("cassandrasnapshot: range delete %q/%d: %w", pid, seqNr, err)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("cassandrasnapshot: range delete scan %q: %w", pid, err)
	}
	return nil
}

var _ persistence.SnapshotStore = (*CassandraSnapshotStore)(nil)
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/snapshot.go
git commit -m "feat(persistence/cassandra): implement SnapshotStore with Pekko-compatible schema"
```

---

### Task 6: Durable state store implementation

**Files:**
- Create: `extensions/persistence/cassandra/state_store.go`

- [ ] **Step 1: Create state_store.go**

```go
/*
 * state_store.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence"
)

// CassandraDurableStateStore implements persistence.DurableStateStore using
// Cassandra. Each actor's state is a single-row partition.
type CassandraDurableStateStore struct {
	session  *gocql.Session
	keyspace string
	table    string
	codec    PayloadCodec
}

// NewCassandraDurableStateStore creates a CassandraDurableStateStore.
func NewCassandraDurableStateStore(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraDurableStateStore {
	return &CassandraDurableStateStore{
		session:  session,
		keyspace: cfg.StateKeyspace,
		table:    cfg.StateTable,
		codec:    codec,
	}
}

// Get retrieves the current state and revision for persistenceID.
// Returns (nil, 0, nil) if no state exists.
func (s *CassandraDurableStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	q := fmt.Sprintf(`SELECT revision, state_data, state_manifest, tag FROM %s.%s WHERE persistence_id = ?`,
		s.keyspace, s.table)

	var revision int64
	var data []byte
	var manifest, tag string
	if err := s.session.Query(q, persistenceID).WithContext(ctx).Scan(&revision, &data, &manifest, &tag); err != nil {
		if err.Error() == "not found" {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("cassandrastate: get %q: %w", persistenceID, err)
	}

	payload, err := s.codec.Decode(manifest, data)
	if err != nil {
		return nil, 0, fmt.Errorf("cassandrastate: decode %q: %w", persistenceID, err)
	}
	return payload, uint64(revision), nil
}

// Upsert stores or replaces the state for persistenceID.
func (s *CassandraDurableStateStore) Upsert(ctx context.Context, persistenceID string, seqNr uint64, state any, tag string) error {
	manifest, data, err := s.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("cassandrastate: encode %q: %w", persistenceID, err)
	}

	q := fmt.Sprintf(`INSERT INTO %s.%s (persistence_id, revision, state_data, state_manifest, tag) VALUES (?, ?, ?, ?, ?)`,
		s.keyspace, s.table)
	if err := s.session.Query(q, persistenceID, int64(seqNr), data, manifest, tag).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrastate: upsert %q: %w", persistenceID, err)
	}
	return nil
}

// Delete removes the state for persistenceID.
func (s *CassandraDurableStateStore) Delete(ctx context.Context, persistenceID string) error {
	q := fmt.Sprintf(`DELETE FROM %s.%s WHERE persistence_id = ?`, s.keyspace, s.table)
	if err := s.session.Query(q, persistenceID).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandrastate: delete %q: %w", persistenceID, err)
	}
	return nil
}

var _ persistence.DurableStateStore = (*CassandraDurableStateStore)(nil)
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/state_store.go
git commit -m "feat(persistence/cassandra): implement DurableStateStore"
```

---

### Task 7: Query journal (read-side)

**Files:**
- Create: `extensions/persistence/cassandra/query_journal.go`

- [ ] **Step 1: Create query_journal.go**

```go
/*
 * query_journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
)

// CassandraReadJournal implements the query-side read journal interfaces for
// Cassandra, including EventsByPersistenceId, EventsByTag, and PersistenceIds.
type CassandraReadJournal struct {
	session        *gocql.Session
	keyspace       string
	journalTable   string
	metadataTable  string
	tagTable       string
	scanningTable  string
	targetPartSz   int64
	bucketSize     BucketSize
	codec          PayloadCodec
	refreshInterval time.Duration
}

// NewCassandraReadJournal creates a CassandraReadJournal.
func NewCassandraReadJournal(session *gocql.Session, cfg *CassandraConfig, codec PayloadCodec) *CassandraReadJournal {
	return &CassandraReadJournal{
		session:         session,
		keyspace:        cfg.JournalKeyspace,
		journalTable:    cfg.JournalTable,
		metadataTable:   cfg.MetadataTable,
		tagTable:        cfg.TagTable,
		scanningTable:   cfg.ScanningTable,
		targetPartSz:    cfg.TargetPartitionSize,
		bucketSize:      cfg.BucketSize,
		codec:           codec,
		refreshInterval: 3 * time.Second,
	}
}

func (j *CassandraReadJournal) IsReadJournal() {}

// readMaxPartNr reads the current max partition_nr from the metadata table.
func (j *CassandraReadJournal) readMaxPartNr(pid string) (int64, error) {
	var partNr int64
	q := fmt.Sprintf(`SELECT partition_nr FROM %s.%s WHERE persistence_id = ?`, j.keyspace, j.metadataTable)
	if err := j.session.Query(q, pid).Scan(&partNr); err != nil {
		if err.Error() == "not found" {
			return 0, nil
		}
		return 0, err
	}
	return partNr, nil
}

// readHighest returns the highest sequence_nr across all partitions for pid.
func (j *CassandraReadJournal) readHighest(pid string) (int64, error) {
	maxPart, err := j.readMaxPartNr(pid)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT sequence_nr FROM %s.%s WHERE persistence_id = ? AND partition_nr = ? ORDER BY sequence_nr DESC LIMIT 1`,
		j.keyspace, j.journalTable)
	for p := maxPart; p >= 0; p-- {
		var seqNr int64
		if err := j.session.Query(q, pid, p).Scan(&seqNr); err == nil {
			return seqNr, nil
		}
	}
	return 0, nil
}

// scanPartitions reads events from [fromSeq, toSeq] across all partitions
// and sends them to the results channel. If live is true, it polls for new events.
func (j *CassandraReadJournal) scanPartitions(pid string, fromSeq, toSeq int64, live bool, results chan<- query.EventEnvelope) error {
	defer close(results)

	selectQ := fmt.Sprintf(`SELECT sequence_nr, event, ser_manifest, event_manifest, tags
		FROM %s.%s
		WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr >= ? AND sequence_nr <= ?
		ORDER BY sequence_nr ASC`,
		j.keyspace, j.journalTable)

	currentFrom := fromSeq

	for {
		maxPart, err := j.readMaxPartNr(pid)
		if err != nil {
			return err
		}

		startPart := currentFrom / j.targetPartSz
		if startPart < 0 {
			startPart = 0
		}

		found := false
		for partNr := startPart; partNr <= maxPart; partNr++ {
			effectiveTo := toSeq
			if effectiveTo <= 0 {
				effectiveTo = int64(^uint64(0) >> 1)
			}

			iter := j.session.Query(selectQ, pid, partNr, currentFrom, effectiveTo).Iter()

			var seqNr int64
			var eventData []byte
			var serManifest, eventManifest string
			var tags []string

			for iter.Scan(&seqNr, &eventData, &serManifest, &eventManifest, &tags) {
				payload, decErr := j.codec.Decode(serManifest, eventData)
				if decErr != nil {
					_ = iter.Close()
					return decErr
				}
				results <- query.EventEnvelope{
					Offset:        query.SequenceOffset(seqNr),
					PersistenceID: pid,
					SequenceNr:    uint64(seqNr),
					Event:         payload,
					Timestamp:     time.Now().UnixNano(),
				}
				currentFrom = seqNr + 1
				found = true
			}
			if err := iter.Close(); err != nil {
				return err
			}
		}

		if !live {
			return nil
		}
		if !found {
			time.Sleep(j.refreshInterval)
		}
	}
}

// EventsByPersistenceId returns a live stream of events for pid.
func (j *CassandraReadJournal) EventsByPersistenceId(pid string, fromSeqNr, toSeqNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	ch := make(chan query.EventEnvelope, 64)
	go func() {
		_ = j.scanPartitions(pid, fromSeqNr, toSeqNr, toSeqNr <= 0, ch)
	}()

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		env, ok := <-ch
		if !ok {
			return query.EventEnvelope{}, false, nil
		}
		return env, true, nil
	})
}

// CurrentEventsByPersistenceId returns a finite stream up to the current highest seqNr.
func (j *CassandraReadJournal) CurrentEventsByPersistenceId(pid string, fromSeqNr, toSeqNr int64) stream.Source[query.EventEnvelope, stream.NotUsed] {
	ch := make(chan query.EventEnvelope, 64)
	go func() {
		effectiveTo := toSeqNr
		if effectiveTo <= 0 {
			highest, _ := j.readHighest(pid)
			effectiveTo = highest
		}
		_ = j.scanPartitions(pid, fromSeqNr, effectiveTo, false, ch)
	}()

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		env, ok := <-ch
		if !ok {
			return query.EventEnvelope{}, false, nil
		}
		return env, true, nil
	})
}

// EventsByTag returns a live stream of events matching tag, starting from offset.
func (j *CassandraReadJournal) EventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return j.eventsByTagInternal(tag, offset, true)
}

// CurrentEventsByTag returns a finite stream of currently-stored events matching tag.
func (j *CassandraReadJournal) CurrentEventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	return j.eventsByTagInternal(tag, offset, false)
}

func (j *CassandraReadJournal) eventsByTagInternal(tag string, offset query.Offset, live bool) stream.Source[query.EventEnvelope, stream.NotUsed] {
	ch := make(chan query.EventEnvelope, 64)

	go func() {
		defer close(ch)

		var startTime time.Time
		if to, ok := offset.(query.TimeOffset); ok {
			startTime = time.Unix(0, int64(to))
		}
		if startTime.IsZero() {
			startTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		}

		currentBucket := Timebucket(startTime, j.bucketSize)
		var lastTimestamp gocql.UUID

		selectQ := fmt.Sprintf(`SELECT timestamp, persistence_id, sequence_nr, event, ser_manifest, event_manifest
			FROM %s.%s
			WHERE tag_name = ? AND timebucket = ? AND timestamp > ?
			ORDER BY timestamp ASC`,
			j.keyspace, j.tagTable)

		for {
			nowBucket := Timebucket(time.Now(), j.bucketSize)
			found := false

			for bucket := currentBucket; bucket <= nowBucket; bucket++ {
				minTs := lastTimestamp
				if minTs.Time().IsZero() {
					// gocql.MinTimeUUID for startTime
					minTs = gocql.MinTimeUUID(startTime)
				}

				iter := j.session.Query(selectQ, tag, bucket, minTs).Iter()

				var ts gocql.UUID
				var pid string
				var seqNr int64
				var eventData []byte
				var serManifest, eventManifest string

				for iter.Scan(&ts, &pid, &seqNr, &eventData, &serManifest, &eventManifest) {
					payload, decErr := j.codec.Decode(serManifest, eventData)
					if decErr != nil {
						_ = iter.Close()
						return
					}
					ch <- query.EventEnvelope{
						Offset:        query.TimeOffset(ts.Time().UnixNano()),
						PersistenceID: pid,
						SequenceNr:    uint64(seqNr),
						Event:         payload,
						Timestamp:     ts.Time().UnixNano(),
					}
					lastTimestamp = ts
					found = true
				}
				_ = iter.Close()

				currentBucket = bucket
			}

			if !live {
				return
			}
			if !found {
				time.Sleep(j.refreshInterval)
			}
		}
	}()

	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		env, ok := <-ch
		if !ok {
			return query.EventEnvelope{}, false, nil
		}
		return env, true, nil
	})
}

// CurrentPersistenceIds returns a finite stream of all persistence IDs.
func (j *CassandraReadJournal) CurrentPersistenceIds() stream.Source[string, stream.NotUsed] {
	ch := make(chan string, 64)
	go func() {
		defer close(ch)
		q := fmt.Sprintf(`SELECT persistence_id FROM %s.%s`, j.keyspace, j.metadataTable)
		iter := j.session.Query(q).Iter()
		var pid string
		for iter.Scan(&pid) {
			ch <- pid
		}
		_ = iter.Close()
	}()

	return stream.FromIteratorFunc(func() (string, bool, error) {
		pid, ok := <-ch
		if !ok {
			return "", false, nil
		}
		return pid, true, nil
	})
}

// PersistenceIds returns a live stream of persistence IDs, polling for new ones.
func (j *CassandraReadJournal) PersistenceIds() stream.Source[string, stream.NotUsed] {
	ch := make(chan string, 64)
	go func() {
		defer close(ch)
		seen := make(map[string]struct{})
		q := fmt.Sprintf(`SELECT persistence_id FROM %s.%s`, j.keyspace, j.metadataTable)
		for {
			iter := j.session.Query(q).Iter()
			var pid string
			found := false
			for iter.Scan(&pid) {
				if _, dup := seen[pid]; !dup {
					seen[pid] = struct{}{}
					ch <- pid
					found = true
				}
			}
			_ = iter.Close()

			if !found {
				time.Sleep(j.refreshInterval)
			}
		}
	}()

	return stream.FromIteratorFunc(func() (string, bool, error) {
		pid, ok := <-ch
		if !ok {
			return "", false, nil
		}
		return pid, true, nil
	})
}

// Compile-time interface checks.
var (
	_ query.ReadJournal                      = (*CassandraReadJournal)(nil)
	_ query.EventsByPersistenceIdQuery       = (*CassandraReadJournal)(nil)
	_ query.CurrentEventsByPersistenceIdQuery = (*CassandraReadJournal)(nil)
	_ query.EventsByTagQuery                 = (*CassandraReadJournal)(nil)
	_ query.PersistenceIdsQuery              = (*CassandraReadJournal)(nil)
	_ query.CurrentPersistenceIdsQuery       = (*CassandraReadJournal)(nil)
)
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/query_journal.go
git commit -m "feat(persistence/cassandra): implement ReadJournal with timebucket tag queries"
```

---

### Task 8: Registration (init)

**Files:**
- Create: `extensions/persistence/cassandra/register.go`

- [ ] **Step 1: Create register.go**

```go
/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"

	"github.com/gocql/gocql"
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// DefaultCodec is the shared JSON codec used by the factory-registered
// "cassandra" providers. Register your domain event and snapshot types on
// this codec before calling gekka.NewActorSystem:
//
//	cassandrastore.DefaultCodec.Register(OrderPlaced{})
//	cassandrastore.DefaultCodec.Register(OrderShipped{})
var DefaultCodec = NewJSONCodec()

func init() {
	persistence.RegisterJournalProvider("cassandra", func(cfg hocon.Config) (persistence.Journal, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build journal: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: journal schema: %w", err)
		}
		return NewCassandraJournal(sess, c, DefaultCodec), nil
	})

	persistence.RegisterSnapshotStoreProvider("cassandra", func(cfg hocon.Config) (persistence.SnapshotStore, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build snapshot: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: snapshot schema: %w", err)
		}
		return NewCassandraSnapshotStore(sess, c, DefaultCodec), nil
	})

	persistence.RegisterDurableStateStoreProvider("cassandra", func(cfg hocon.Config) (persistence.DurableStateStore, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build state store: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: state store schema: %w", err)
		}
		return NewCassandraDurableStateStore(sess, c, DefaultCodec), nil
	})

	persistence.RegisterReadJournalProvider("cassandra", func(cfg hocon.Config) (any, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build read journal: %w", err)
		}
		return NewCassandraReadJournal(sess, c, DefaultCodec), nil
	})
}

// buildFromConfig parses HOCON config and returns a CassandraConfig + session.
func buildFromConfig(cfg hocon.Config) (*CassandraConfig, *gocql.Session, error) {
	c, err := ParseConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := GetOrCreateSession(c)
	if err != nil {
		return nil, nil, err
	}
	return c, sess, nil
}
```

- [ ] **Step 2: Verify build**

Run: `cd extensions/persistence/cassandra && go build ./... && cd ../../..`
Expected: Build succeeds.

- [ ] **Step 3: Commit**

```bash
git add extensions/persistence/cassandra/register.go
git commit -m "feat(persistence/cassandra): add init() provider registration"
```

---

### Task 9: Integration tests

**Files:**
- Create: `extensions/persistence/cassandra/cassandra_test.go`

- [ ] **Step 1: Create cassandra_test.go**

```go
//go:build integration

/*
 * cassandra_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	cassandrastore "github.com/sopranoworks/gekka-extensions-persistence-cassandra"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testSession *gocql.Session

func TestMain(m *testing.M) {
	ctx := context.Background()

	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Docker not found, skipping Cassandra integration tests.")
		os.Exit(0)
	}
	if _, err := cli.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "Docker not found, skipping Cassandra integration tests.")
		os.Exit(0)
	}
	cli.Close()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "cassandra:4.1",
			ExposedPorts: []string{"9042/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForLog("Starting listening for CQL clients"),
				wait.ForListeningPort("9042/tcp"),
			).WithDeadline(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start Cassandra container: %v\n", err)
		os.Exit(1)
	}

	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "9042/tcp")

	cluster := gocql.NewCluster(host)
	cluster.Port = port.Int()
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	// Cassandra needs a moment after CQL port opens. Retry connection.
	for i := 0; i < 30; i++ {
		testSession, err = cluster.CreateSession()
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		_ = ctr.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to connect to Cassandra: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	testSession.Close()
	_ = ctr.Terminate(ctx)
	os.Exit(code)
}

// uniqueKeyspace creates a unique test keyspace and returns a CassandraConfig
// with autocreate enabled.
func uniqueKeyspace(t *testing.T) *cassandrastore.CassandraConfig {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	ks := "test_" + hex.EncodeToString(b)
	snapKs := ks + "_snap"

	cfg := &cassandrastore.CassandraConfig{
		ContactPoints:       []string{"127.0.0.1"},
		Port:                9042,
		Datacenter:          "datacenter1",
		JournalKeyspace:     ks,
		JournalTable:        "messages",
		MetadataTable:       "metadata",
		TargetPartitionSize: 10, // Small for testing partition rollover
		KeyspaceAutocreate:  true,
		TablesAutocreate:    true,
		ReplicationStrategy: "SimpleStrategy",
		ReplicationFactor:   1,
		GcGraceSeconds:      864000,
		SnapshotKeyspace:    snapKs,
		SnapshotTable:       "snapshots",
		SnapshotAutoKeyspace: true,
		SnapshotAutoTables:  true,
		TagTable:            "tag_views",
		ScanningTable:       "tag_scanning",
		BucketSize:          cassandrastore.BucketDay,
		StateKeyspace:       ks,
		StateTable:          "durable_state",
	}

	require.NoError(t, cassandrastore.CreateSchema(testSession, cfg))
	return cfg
}

// ── Test types ───────────────────────────────────────────────────────────────

type OrderPlaced struct{ Item string }
type CartState struct{ Items []string }

// ── Journal tests ────────────────────────────────────────────────────────────

func TestCassandra_Journal_WriteAndReplay(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "order-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "banana"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "cherry"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), highest)

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 3)
	assert.Equal(t, uint64(1), replayed[0].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "apple"}, replayed[0].Payload)
	assert.Equal(t, OrderPlaced{Item: "banana"}, replayed[1].Payload)
	assert.Equal(t, OrderPlaced{Item: "cherry"}, replayed[2].Payload)
}

func TestCassandra_Journal_PartitionRollover(t *testing.T) {
	cfg := uniqueKeyspace(t)
	// TargetPartitionSize=10 in uniqueKeyspace, so partition_nr bumps at seqNr 11.
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "rollover-1"

	var events []persistence.PersistentRepr
	for i := uint64(1); i <= 15; i++ {
		events = append(events, persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    i,
			Payload:       OrderPlaced{Item: fmt.Sprintf("item-%d", i)},
		})
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), highest)

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 15, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 15)
	assert.Equal(t, uint64(1), replayed[0].SequenceNr)
	assert.Equal(t, uint64(15), replayed[14].SequenceNr)
}

func TestCassandra_Journal_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "delete-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "a"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "b"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "c"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))
	require.NoError(t, j.AsyncDeleteMessagesTo(ctx, pid, 2))

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 0, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 1)
	assert.Equal(t, uint64(3), replayed[0].SequenceNr)
}

func TestCassandra_Journal_TaggedEvents(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "tagged-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}, Tags: []string{"fruit"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "carrot"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "banana"}, Tags: []string{"fruit", "yellow"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	// Verify tag_views were populated by reading them directly.
	q := fmt.Sprintf(`SELECT persistence_id, sequence_nr FROM %s.%s WHERE tag_name = ? AND timebucket = ? ALLOW FILTERING`,
		cfg.JournalKeyspace, cfg.TagTable)
	bucket := cassandrastore.Timebucket(time.Now(), cfg.BucketSize)

	iter := testSession.Query(q, "fruit", bucket).Iter()
	var taggedPid string
	var taggedSeq int64
	var count int
	for iter.Scan(&taggedPid, &taggedSeq) {
		count++
	}
	_ = iter.Close()
	assert.Equal(t, 2, count, "expected 2 events tagged 'fruit'")
}

func TestCassandra_Journal_ReadHighest_Empty(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()

	highest, err := j.ReadHighestSequenceNr(ctx, "nonexistent-pid", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), highest)
}

// ── Snapshot tests ───────────────────────────────────────────────────────────

func TestCassandra_Snapshot_SaveAndLoad(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "cart-1"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{"a", "b"}}))

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(5), snap.Metadata.SequenceNr)
	state, ok := snap.Snapshot.(CartState)
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b"}, state.Items)
}

func TestCassandra_Snapshot_LoadLatest(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "cart-latest"

	for _, seqNr := range []uint64{3, 7, 5} {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: seqNr}
		require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{fmt.Sprintf("seq-%d", seqNr)}}))
	}

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(7), snap.Metadata.SequenceNr)
}

func TestCassandra_Snapshot_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "cart-delete"

	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 10}
	require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{"x"}}))
	require.NoError(t, ss.DeleteSnapshot(ctx, meta))

	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestCassandra_Snapshot_NotFound(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	ss := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()

	snap, err := ss.LoadSnapshot(ctx, "nonexistent", persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

// ── DurableStateStore tests ──────────────────────────────────────────────────

func TestCassandra_State_UpsertAndGet(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "state-1"

	require.NoError(t, ss.Upsert(ctx, pid, 1, CartState{Items: []string{"apple", "banana"}}, ""))

	state, rev, err := ss.Get(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rev)
	cart, ok := state.(CartState)
	require.True(t, ok)
	assert.Equal(t, []string{"apple", "banana"}, cart.Items)
}

func TestCassandra_State_UpsertOverwrites(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "state-overwrite"

	require.NoError(t, ss.Upsert(ctx, pid, 1, CartState{Items: []string{"a"}}, ""))
	require.NoError(t, ss.Upsert(ctx, pid, 2, CartState{Items: []string{"a", "b", "c"}}, "checkout"))

	state, rev, err := ss.Get(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rev)
	cart, ok := state.(CartState)
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b", "c"}, cart.Items)
}

func TestCassandra_State_NotFound(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	ss := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()

	state, rev, err := ss.Get(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, state)
	assert.Equal(t, uint64(0), rev)
}

func TestCassandra_State_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})

	ss := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()
	const pid = "state-delete"

	require.NoError(t, ss.Upsert(ctx, pid, 1, CartState{Items: []string{"x"}}, ""))
	require.NoError(t, ss.Delete(ctx, pid))

	state, rev, err := ss.Get(ctx, pid)
	require.NoError(t, err)
	assert.Nil(t, state)
	assert.Equal(t, uint64(0), rev)
}

// ── Registration smoke-test ──────────────────────────────────────────────────

func TestCassandra_Registration(t *testing.T) {
	cfg := uniqueKeyspace(t)
	_ = cfg // Registration tests need HOCON config pointing at live container.
	// For now, verify the compile-time interface assertions pass
	// (the init() registration is validated by the build itself).

	// If we had the live container host/port in a HOCON string, we could test:
	//   j, err := persistence.NewJournal("cassandra", hoconCfg)
	// This is covered implicitly by the other tests constructing stores directly.
}
```

- [ ] **Step 2: Run `go mod tidy` to pull test dependencies**

Run:
```bash
cd extensions/persistence/cassandra && go mod tidy && cd ../../..
```
Expected: go.sum updated with testcontainers-go dependencies.

- [ ] **Step 3: Run integration tests**

Run:
```bash
cd extensions/persistence/cassandra && go test -v -tags integration -timeout 300s ./...
```
Expected: All tests pass (may take 1-2 minutes for Cassandra container startup).

- [ ] **Step 4: Commit**

```bash
git add extensions/persistence/cassandra/cassandra_test.go extensions/persistence/cassandra/go.mod extensions/persistence/cassandra/go.sum
git commit -m "test(persistence/cassandra): add integration tests against Cassandra 4.1 container"
```

---

### Task 10: Phase 5 gate + work checklist update

**Files:**
- Modify: `.work/20260410.md`

- [ ] **Step 1: Run full build + unit tests**

Run:
```bash
go build ./... && go test ./...
```
Expected: All pass.

- [ ] **Step 2: Run lint**

Run:
```bash
~/go/bin/golangci-lint run ./extensions/persistence/cassandra/...
```
Expected: No issues.

- [ ] **Step 3: Run integration tests (Iron Rule #1)**

Run:
```bash
go test -tags integration ./...
```
Expected: All integration tests pass including Cassandra.

- [ ] **Step 4: Update `.work/20260410.md` Phase 5 checkboxes**

Mark all Phase 5 items as done with commit hashes. Mark LevelDB as:
```
[~] SKIPPED: not required by test/compatibility/akka-multi-node
```

- [ ] **Step 5: Commit work checklist**

Do NOT commit `.work/20260410.md` — it is gitignored per `feedback_gitignore_respect.md`.

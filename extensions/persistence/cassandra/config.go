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

// BucketSize determines the time-bucket granularity used for tag_views partitioning.
type BucketSize int

const (
	// BucketDay partitions tag events by day (86400000 ms).
	BucketDay BucketSize = iota
	// BucketHour partitions tag events by hour (3600000 ms).
	BucketHour
	// BucketMinute partitions tag events by minute (60000 ms).
	BucketMinute
)

// DurationMs returns the duration of the bucket in milliseconds.
func (b BucketSize) DurationMs() int64 {
	switch b {
	case BucketHour:
		return 3_600_000
	case BucketMinute:
		return 60_000
	default: // BucketDay
		return 86_400_000
	}
}

// Timebucket computes the bucket index for time t at the given granularity.
// It returns t.UnixMilli() / size.DurationMs().
func Timebucket(t time.Time, size BucketSize) int64 {
	return t.UnixMilli() / size.DurationMs()
}

// ParseBucketSize parses a human-readable bucket size string.
// Recognised values (case-insensitive): "Hour", "Minute".  Everything else
// (including "Day") returns BucketDay.
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

// CassandraConfig holds all configuration required to connect to Cassandra and
// manage the persistence schema.
type CassandraConfig struct {
	// Connection
	ContactPoints []string
	Port          int
	Datacenter    string
	Username      string
	Password      string

	// Timeouts / pooling
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	NumConnections int

	// Journal keyspace / tables
	JournalKeyspace     string
	JournalTable        string
	MetadataTable       string
	TargetPartitionSize int64

	// Schema auto-creation
	KeyspaceAutocreate  bool
	TablesAutocreate    bool
	ReplicationStrategy string
	ReplicationFactor   int
	GcGraceSeconds      int

	// Snapshot keyspace / tables
	SnapshotKeyspace    string
	SnapshotTable       string
	SnapshotAutoKeyspace bool
	SnapshotAutoTables  bool

	// Tag / scanning tables
	TagTable      string
	ScanningTable string
	BucketSize    BucketSize

	// Durable-state keyspace / table
	StateKeyspace string
	StateTable    string
}

// ValidateConfig checks that all required Cassandra configuration keys are
// present in cfg.  It is called automatically by the registered factories
// before any network connection is attempted.
func ValidateConfig(cfg hocon.Config) error {
	return persistence.ValidateProviderConfig(cfg, "contact-points", "port", "local-datacenter")
}

// ParseConfig fills a CassandraConfig with defaults then overrides from the
// provided HOCON sub-config (the settings block passed to a journal/snapshot
// provider factory).
func ParseConfig(cfg hocon.Config) (*CassandraConfig, error) {
	c := &CassandraConfig{
		Port:                9042,
		Datacenter:          "datacenter1",
		ConnectTimeout:      5 * time.Second,
		ReadTimeout:         12 * time.Second,
		NumConnections:      2,
		JournalKeyspace:     "pekko",
		JournalTable:        "messages",
		MetadataTable:       "metadata",
		TargetPartitionSize: 500_000,
		ReplicationStrategy: "SimpleStrategy",
		ReplicationFactor:   1,
		GcGraceSeconds:      864_000,
		SnapshotKeyspace:    "pekko_snapshot",
		SnapshotTable:       "snapshots",
		TagTable:            "tag_views",
		ScanningTable:       "tag_scanning",
		BucketSize:          BucketDay,
		StateKeyspace:       "pekko",
		StateTable:          "durable_state",
	}

	// contact-points: can be a list or a single comma-separated string
	if v, err := cfg.GetString("contact-points"); err == nil && v != "" {
		parts := strings.Split(v, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		c.ContactPoints = parts
	}

	if v, err := cfg.GetInt("port"); err == nil {
		c.Port = v
	}
	if v, err := cfg.GetString("local-datacenter"); err == nil && v != "" {
		c.Datacenter = v
	}
	if v, err := cfg.GetString("username"); err == nil {
		c.Username = v
	}
	if v, err := cfg.GetString("password"); err == nil {
		c.Password = v
	}

	if v, err := cfg.GetDuration("connect-timeout"); err == nil {
		c.ConnectTimeout = v
	}
	if v, err := cfg.GetDuration("read-timeout"); err == nil {
		c.ReadTimeout = v
	}
	if v, err := cfg.GetInt("num-connections"); err == nil {
		c.NumConnections = v
	}

	if v, err := cfg.GetString("journal.keyspace"); err == nil && v != "" {
		c.JournalKeyspace = v
	}
	if v, err := cfg.GetString("journal.table"); err == nil && v != "" {
		c.JournalTable = v
	}
	if v, err := cfg.GetString("journal.metadata-table"); err == nil && v != "" {
		c.MetadataTable = v
	}
	if v, err := cfg.GetInt("journal.target-partition-size"); err == nil {
		c.TargetPartitionSize = int64(v)
	}

	if v, err := cfg.GetBoolean("keyspace-autocreate"); err == nil {
		c.KeyspaceAutocreate = v
	}
	if v, err := cfg.GetBoolean("tables-autocreate"); err == nil {
		c.TablesAutocreate = v
	}
	if v, err := cfg.GetString("replication-strategy"); err == nil && v != "" {
		c.ReplicationStrategy = v
	}
	if v, err := cfg.GetInt("replication-factor"); err == nil {
		c.ReplicationFactor = v
	}
	if v, err := cfg.GetInt("gc-grace-seconds"); err == nil {
		c.GcGraceSeconds = v
	}

	if v, err := cfg.GetString("snapshot.keyspace"); err == nil && v != "" {
		c.SnapshotKeyspace = v
	}
	if v, err := cfg.GetString("snapshot.table"); err == nil && v != "" {
		c.SnapshotTable = v
	}
	if v, err := cfg.GetBoolean("snapshot.keyspace-autocreate"); err == nil {
		c.SnapshotAutoKeyspace = v
	}
	if v, err := cfg.GetBoolean("snapshot.tables-autocreate"); err == nil {
		c.SnapshotAutoTables = v
	}

	if v, err := cfg.GetString("tag-table"); err == nil && v != "" {
		c.TagTable = v
	}
	if v, err := cfg.GetString("scanning-table"); err == nil && v != "" {
		c.ScanningTable = v
	}
	if v, err := cfg.GetString("bucket-size"); err == nil && v != "" {
		c.BucketSize = ParseBucketSize(v)
	}

	if v, err := cfg.GetString("state.keyspace"); err == nil && v != "" {
		c.StateKeyspace = v
	}
	if v, err := cfg.GetString("state.table"); err == nil && v != "" {
		c.StateTable = v
	}

	return c, nil
}

// ── Session cache ─────────────────────────────────────────────────────────────

var (
	sessionMu    sync.Mutex
	sessionCache = map[string]*gocql.Session{}
)

func sessionKey(cfg *CassandraConfig) string {
	return fmt.Sprintf("%v@%d/%s", cfg.ContactPoints, cfg.Port, cfg.Datacenter)
}

// GetOrCreateSession returns a cached *gocql.Session for the given config,
// creating one on the first call.  Sessions are keyed by contact-points, port,
// and datacenter.
func GetOrCreateSession(cfg *CassandraConfig) (*gocql.Session, error) {
	key := sessionKey(cfg)

	sessionMu.Lock()
	defer sessionMu.Unlock()

	if s, ok := sessionCache[key]; ok {
		return s, nil
	}

	cluster := gocql.NewCluster(cfg.ContactPoints...)
	cluster.Port = cfg.Port
	cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(cfg.Datacenter)
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.Timeout = cfg.ReadTimeout
	cluster.NumConns = cfg.NumConnections

	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandrastore: create session: %w", err)
	}

	sessionCache[key] = session
	return session, nil
}

// ParseConfigString is a convenience wrapper around hocon.ParseString for
// building a settings config in tests without importing the hocon package:
//
//	cfg, _ := cassandrastore.ParseConfigString(`{ contact-points: "localhost", port: 9042, local-datacenter: "datacenter1" }`)
func ParseConfigString(s string) (*hocon.Config, error) {
	return hocon.ParseString(s)
}

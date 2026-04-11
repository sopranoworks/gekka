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
	mobyClient "github.com/moby/moby/client"
	cassandrastore "github.com/sopranoworks/gekka-extensions-persistence-cassandra"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testSession *gocql.Session

// OrderPlaced is a sample event type used in journal tests.
type OrderPlaced struct{ Item string }

// CartState is a sample state type used in snapshot tests.
type CartState struct{ Items []string }

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Check Docker availability.
	dockerClient, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		fmt.Println("Docker not available, skipping integration tests:", err)
		os.Exit(0)
	}
	if _, err := dockerClient.Ping(ctx, mobyClient.PingOptions{}); err != nil {
		fmt.Println("Docker not reachable, skipping integration tests:", err)
		os.Exit(0)
	}

	// Start Cassandra 4.1 container.
	req := testcontainers.ContainerRequest{
		Image:        "cassandra:4.1",
		ExposedPorts: []string{"9042/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog("Starting listening for CQL clients"),
			wait.ForListeningPort("9042/tcp"),
		).WithDeadline(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Println("Failed to start Cassandra container:", err)
		os.Exit(1)
	}
	defer func() {
		_ = container.Terminate(ctx)
	}()

	host, err := container.Host(ctx)
	if err != nil {
		fmt.Println("Failed to get container host:", err)
		os.Exit(1)
	}
	mappedPort, err := container.MappedPort(ctx, "9042/tcp")
	if err != nil {
		fmt.Println("Failed to get mapped port:", err)
		os.Exit(1)
	}
	port := mappedPort.Int()

	// Retry session creation — Cassandra needs warmup time after CQL port opens.
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.Consistency = gocql.Quorum

	var session *gocql.Session
	for i := 0; i < 30; i++ {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		fmt.Println("Failed to create Cassandra session after retries:", err)
		os.Exit(1)
	}

	testSession = session
	code := m.Run()
	session.Close()
	os.Exit(code)
}

// uniqueKeyspace creates a unique keyspace pair for test isolation and sets up
// the schema.  TargetPartitionSize is set to 10 so partition-rollover tests
// work with small datasets.
func uniqueKeyspace(t *testing.T) *cassandrastore.CassandraConfig {
	t.Helper()
	b := make([]byte, 4)
	rand.Read(b) //nolint:errcheck
	ks := "test_" + hex.EncodeToString(b)
	snapKs := ks + "_snap"

	cfg := &cassandrastore.CassandraConfig{
		ContactPoints:        []string{"localhost"},
		Port:                 9042,
		Datacenter:           "datacenter1",
		JournalKeyspace:      ks,
		JournalTable:         "messages",
		MetadataTable:        "metadata",
		TargetPartitionSize:  10,
		ReplicationStrategy:  "SimpleStrategy",
		ReplicationFactor:    1,
		GcGraceSeconds:       864_000,
		SnapshotKeyspace:     snapKs,
		SnapshotTable:        "snapshots",
		TagTable:             "tag_views",
		ScanningTable:        "tag_scanning",
		BucketSize:           cassandrastore.BucketDay,
		StateKeyspace:        ks,
		StateTable:           "durable_state",
		KeyspaceAutocreate:   true,
		TablesAutocreate:     true,
		SnapshotAutoKeyspace: true,
		SnapshotAutoTables:   true,
	}
	require.NoError(t, cassandrastore.CreateSchema(testSession, cfg))
	return cfg
}

// ── Journal tests ─────────────────────────────────────────────────────────────

func TestCassandra_Journal_WriteAndReplay(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})
	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()

	pid := "order-1"
	msgs := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "banana"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "cherry"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, msgs))

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), highest)

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 100, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 3)
	assert.Equal(t, uint64(1), replayed[0].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "apple"}, replayed[0].Payload)
	assert.Equal(t, uint64(2), replayed[1].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "banana"}, replayed[1].Payload)
	assert.Equal(t, uint64(3), replayed[2].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "cherry"}, replayed[2].Payload)
}

func TestCassandra_Journal_PartitionRollover(t *testing.T) {
	cfg := uniqueKeyspace(t) // TargetPartitionSize == 10
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})
	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()

	pid := "order-rollover"
	msgs := make([]persistence.PersistentRepr, 15)
	for i := range msgs {
		msgs[i] = persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    uint64(i + 1), //nolint:gosec
			Payload:       OrderPlaced{Item: fmt.Sprintf("item-%d", i+1)},
		}
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, msgs))

	highest, err := j.ReadHighestSequenceNr(ctx, pid, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), highest)

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 15, 100, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 15)
	for i, r := range replayed {
		assert.Equal(t, uint64(i+1), r.SequenceNr) //nolint:gosec
	}
}

func TestCassandra_Journal_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})
	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()

	pid := "order-delete"
	msgs := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "a"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "b"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "c"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, msgs))
	require.NoError(t, j.AsyncDeleteMessagesTo(ctx, pid, 2))

	var replayed []persistence.PersistentRepr
	require.NoError(t, j.ReplayMessages(ctx, pid, 1, 3, 100, func(r persistence.PersistentRepr) {
		replayed = append(replayed, r)
	}))
	require.Len(t, replayed, 1)
	assert.Equal(t, uint64(3), replayed[0].SequenceNr)
	assert.Equal(t, OrderPlaced{Item: "c"}, replayed[0].Payload)
}

func TestCassandra_Journal_TaggedEvents(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})
	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	ctx := context.Background()

	pid := "order-tagged"
	msgs := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}, Tags: []string{"fruit"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "carrot"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "banana"}, Tags: []string{"fruit"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, msgs))

	// Verify tag_views has 2 entries for "fruit".
	tb := cassandrastore.Timebucket(time.Now(), cassandrastore.BucketDay)
	q := fmt.Sprintf(
		`SELECT count(*) FROM %s.tag_views WHERE tag_name = ? AND timebucket = ?`,
		cfg.JournalKeyspace,
	)
	var count int
	require.NoError(t, testSession.Query(q, "fruit", tb).WithContext(ctx).Scan(&count))
	assert.Equal(t, 2, count)
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

// ── Snapshot tests ────────────────────────────────────────────────────────────

func TestCassandra_Snapshot_SaveAndLoad(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	s := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "cart-1"
	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	state := CartState{Items: []string{"apple", "banana"}}
	require.NoError(t, s.SaveSnapshot(ctx, meta, state))

	snap, err := s.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(5), snap.Metadata.SequenceNr)
	assert.Equal(t, state, snap.Snapshot)
}

func TestCassandra_Snapshot_LoadLatest(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	s := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "cart-latest"
	for _, seqNr := range []uint64{3, 7, 5} {
		require.NoError(t, s.SaveSnapshot(ctx,
			persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: seqNr},
			CartState{Items: []string{fmt.Sprintf("seq-%d", seqNr)}},
		))
	}

	snap, err := s.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(7), snap.Metadata.SequenceNr)
}

func TestCassandra_Snapshot_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	s := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "cart-delete"
	meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: 5}
	require.NoError(t, s.SaveSnapshot(ctx, meta, CartState{Items: []string{"x"}}))
	require.NoError(t, s.DeleteSnapshot(ctx, meta))

	snap, err := s.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestCassandra_Snapshot_NotFound(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	s := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()

	snap, err := s.LoadSnapshot(ctx, "nonexistent-cart", persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	assert.Nil(t, snap)
}

// ── DurableState tests ────────────────────────────────────────────────────────

func TestCassandra_State_UpsertAndGet(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	ds := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "state-1"
	state := CartState{Items: []string{"apple"}}
	require.NoError(t, ds.Upsert(ctx, pid, 1, state, ""))

	got, rev, err := ds.Get(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rev)
	assert.Equal(t, state, got)
}

func TestCassandra_State_UpsertOverwrites(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	ds := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "state-overwrite"
	require.NoError(t, ds.Upsert(ctx, pid, 1, CartState{Items: []string{"a"}}, ""))
	latest := CartState{Items: []string{"a", "b"}}
	require.NoError(t, ds.Upsert(ctx, pid, 2, latest, ""))

	got, rev, err := ds.Get(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rev)
	assert.Equal(t, latest, got)
}

func TestCassandra_State_NotFound(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	ds := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()

	got, rev, err := ds.Get(ctx, "nonexistent-state")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, uint64(0), rev)
}

func TestCassandra_State_Delete(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	ds := cassandrastore.NewCassandraDurableStateStore(testSession, cfg, codec)
	ctx := context.Background()

	pid := "state-delete"
	require.NoError(t, ds.Upsert(ctx, pid, 1, CartState{Items: []string{"x"}}, ""))
	require.NoError(t, ds.Delete(ctx, pid))

	got, rev, err := ds.Get(ctx, pid)
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, uint64(0), rev)
}

// ── Snapshot batch delete test ───────────────────────────────────────────────

func TestCassandra_Snapshot_DeleteSnapshots(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(CartState{})
	ss := cassandrastore.NewCassandraSnapshotStore(testSession, cfg, codec)
	ctx := context.Background()
	pid := "cart-batch-delete"

	for _, seqNr := range []uint64{1, 3, 5, 7, 9} {
		meta := persistence.SnapshotMetadata{PersistenceID: pid, SequenceNr: seqNr}
		require.NoError(t, ss.SaveSnapshot(ctx, meta, CartState{Items: []string{fmt.Sprintf("s%d", seqNr)}}))
	}

	// Delete snapshots with seqNr <= 5.
	require.NoError(t, ss.DeleteSnapshots(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 5,
	}))

	// Only seqNr 7 and 9 should remain.
	snap, err := ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(9), snap.Metadata.SequenceNr)

	// seqNr 5 should be gone.
	snap5, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 5,
	})
	require.NoError(t, err)
	assert.Nil(t, snap5)
}

// ── Query journal tests ──────────────────────────────────────────────────────

func TestCassandra_CurrentEventsByPersistenceId(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	rj := cassandrastore.NewCassandraReadJournal(testSession, cfg, codec)
	ctx := context.Background()
	pid := "query-pid-1"

	var events []persistence.PersistentRepr
	for i := uint64(1); i <= 5; i++ {
		events = append(events, persistence.PersistentRepr{
			PersistenceID: pid,
			SequenceNr:    i,
			Payload:       OrderPlaced{Item: fmt.Sprintf("item-%d", i)},
		})
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	src := rj.CurrentEventsByPersistenceId(pid, 1, 0)
	result, err := stream.RunWith[query.EventEnvelope, stream.NotUsed, []query.EventEnvelope](src, stream.Collect[query.EventEnvelope](), nil)
	require.NoError(t, err)
	require.Len(t, result, 5)
	assert.Equal(t, uint64(1), result[0].SequenceNr)
	assert.Equal(t, uint64(5), result[4].SequenceNr)
}

func TestCassandra_CurrentEventsByTag(t *testing.T) {
	cfg := uniqueKeyspace(t)
	codec := cassandrastore.NewJSONCodec()
	codec.Register(OrderPlaced{})

	j := cassandrastore.NewCassandraJournal(testSession, cfg, codec)
	rj := cassandrastore.NewCassandraReadJournal(testSession, cfg, codec)
	ctx := context.Background()
	pid := "query-tag-1"

	events := []persistence.PersistentRepr{
		{PersistenceID: pid, SequenceNr: 1, Payload: OrderPlaced{Item: "apple"}, Tags: []string{"fruit"}},
		{PersistenceID: pid, SequenceNr: 2, Payload: OrderPlaced{Item: "carrot"}},
		{PersistenceID: pid, SequenceNr: 3, Payload: OrderPlaced{Item: "banana"}, Tags: []string{"fruit"}},
		{PersistenceID: pid, SequenceNr: 4, Payload: OrderPlaced{Item: "potato"}},
	}
	require.NoError(t, j.AsyncWriteMessages(ctx, events))

	// Small delay to ensure tag_views writes are visible.
	time.Sleep(500 * time.Millisecond)

	src := rj.CurrentEventsByTag("fruit", query.NoOffset{})
	result, err := stream.RunWith[query.EventEnvelope, stream.NotUsed, []query.EventEnvelope](src, stream.Collect[query.EventEnvelope](), nil)
	require.NoError(t, err)
	require.Len(t, result, 2)
	// Both should be fruit-tagged events.
	for _, env := range result {
		ev, ok := env.Event.(OrderPlaced)
		require.True(t, ok)
		assert.Contains(t, []string{"apple", "banana"}, ev.Item)
	}
}

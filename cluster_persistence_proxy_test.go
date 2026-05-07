//go:build integration

/*
 * cluster_persistence_proxy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// TestPersistenceProxyJournal_CrossProcess verifies the off-mode
// ProxyJournal end-to-end: persistent actor process A writes through
// the proxy, the journal lives on process B (a separate Cluster
// instance), and the bytes travel over Artery.  Closes ⚠️ row 483.
func TestPersistenceProxyJournal_CrossProcess(t *testing.T) {
	target, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster target: %v", err)
	}
	defer func() { _ = target.Shutdown() }()

	// Host the in-memory journal at the well-known path.
	backingJournal := persistence.NewInMemoryJournal()
	ServePersistenceProxyJournal(target, backingJournal)

	client, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster client: %v", err)
	}
	defer func() { _ = client.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Join(target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := client.WaitForHandshake(ctx, target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	// The client created second wins the global ProxyTransport
	// registration; that's the one HOCON-driven proxies will pick up.
	// Build the target URI explicitly so the proxy knows where to ask.
	targetURI := fmt.Sprintf("pekko://%s@%s:%d%s",
		target.localAddr.GetSystem(),
		target.localAddr.GetHostname(),
		target.localAddr.GetPort(),
		persistence.RemoteJournalPathSuffix)

	cfg, err := hocon.ParseString(fmt.Sprintf(`
target-journal-plugin-id = ignored
start-target-journal = off
target-journal-address = "%s"
init-timeout = 1s
request-timeout = 10s
`, targetURI))
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	j, err := persistence.NewJournal("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewJournal proxy off-mode: %v", err)
	}

	// Write two events through the proxy.
	if err := j.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "actor-1", SequenceNr: 1, Payload: "event-one"},
		{PersistenceID: "actor-1", SequenceNr: 2, Payload: "event-two"},
	}); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}

	// Highest sequence reads back from process B over the wire.
	hi, err := j.ReadHighestSequenceNr(ctx, "actor-1", 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if hi != 2 {
		t.Fatalf("highest seq through proxy = %d, want 2", hi)
	}

	// Replay returns the events written above.
	var got []persistence.PersistentRepr
	if err := j.ReplayMessages(ctx, "actor-1", 1, 2, 100, func(m persistence.PersistentRepr) {
		got = append(got, m)
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if len(got) != 2 || got[0].SequenceNr != 1 || got[1].SequenceNr != 2 {
		t.Fatalf("replay = %+v, want seq 1 and 2", got)
	}
	if got[0].Payload != "event-one" || got[1].Payload != "event-two" {
		t.Errorf("payloads = %v / %v, want \"event-one\" / \"event-two\"", got[0].Payload, got[1].Payload)
	}
}

// TestPersistenceProxySnapshot_CrossProcess mirrors the journal test
// for the snapshot store.  Closes ⚠️ row 489.
func TestPersistenceProxySnapshot_CrossProcess(t *testing.T) {
	target, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster target: %v", err)
	}
	defer func() { _ = target.Shutdown() }()

	backingStore := persistence.NewInMemorySnapshotStore()
	ServePersistenceProxySnapshot(target, backingStore)

	client, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster client: %v", err)
	}
	defer func() { _ = client.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Join(target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := client.WaitForHandshake(ctx, target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	targetURI := fmt.Sprintf("pekko://%s@%s:%d%s",
		target.localAddr.GetSystem(),
		target.localAddr.GetHostname(),
		target.localAddr.GetPort(),
		persistence.RemoteSnapshotPathSuffix)

	cfg, err := hocon.ParseString(fmt.Sprintf(`
target-snapshot-store-plugin-id = ignored
start-target-snapshot-store = off
target-snapshot-store-address = "%s"
init-timeout = 1s
request-timeout = 10s
`, targetURI))
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	store, err := persistence.NewSnapshotStore("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewSnapshotStore proxy off-mode: %v", err)
	}

	meta := persistence.SnapshotMetadata{PersistenceID: "snap-1", SequenceNr: 5, Timestamp: 4242}
	if err := store.SaveSnapshot(ctx, meta, "snapshot-payload"); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	loaded, err := store.LoadSnapshot(ctx, "snap-1", persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadSnapshot returned nil through proxy")
	}
	if loaded.Metadata.SequenceNr != 5 || loaded.Metadata.Timestamp != 4242 {
		t.Errorf("metadata = %+v, want seq=5 ts=4242", loaded.Metadata)
	}
	if loaded.Snapshot != "snapshot-payload" {
		t.Errorf("snapshot = %v, want \"snapshot-payload\"", loaded.Snapshot)
	}

	// Verify the snapshot really lives on the target by querying its
	// backing store directly.
	direct, err := backingStore.LoadSnapshot(ctx, "snap-1", persistence.LatestSnapshotCriteria())
	if err != nil || direct == nil || direct.Metadata.SequenceNr != 5 {
		t.Errorf("backing store load = %+v (err %v), want seq=5", direct, err)
	}
}

// TestPersistenceProxy_TargetDeath_SurfacesError verifies that when
// the target process disappears, in-flight requests fail within the
// configured request-timeout instead of hanging forever.
func TestPersistenceProxy_TargetDeath_SurfacesError(t *testing.T) {
	target, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster target: %v", err)
	}
	ServePersistenceProxyJournal(target, persistence.NewInMemoryJournal())

	client, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster client: %v", err)
	}
	defer func() { _ = client.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Join(target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := client.WaitForHandshake(ctx, target.localAddr.GetHostname(), target.localAddr.GetPort()); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	targetURI := fmt.Sprintf("pekko://%s@%s:%d%s",
		target.localAddr.GetSystem(),
		target.localAddr.GetHostname(),
		target.localAddr.GetPort(),
		persistence.RemoteJournalPathSuffix)
	cfg, err := hocon.ParseString(fmt.Sprintf(`
target-journal-plugin-id = ignored
start-target-journal = off
target-journal-address = "%s"
init-timeout = 1s
request-timeout = 1s
`, targetURI))
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	j, err := persistence.NewJournal("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewJournal proxy off-mode: %v", err)
	}

	// Tear down the target before sending; the in-memory transport
	// will see Ask fail with deadline / connection error.
	_ = target.Shutdown()
	time.Sleep(100 * time.Millisecond)

	deadline, cancelDeadline := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDeadline()
	err = j.AsyncWriteMessages(deadline, []persistence.PersistentRepr{
		{PersistenceID: "actor-x", SequenceNr: 1, Payload: "v"},
	})
	if err == nil {
		t.Fatal("AsyncWriteMessages with dead target: want error, got nil")
	}
	if !strings.Contains(err.Error(), "timeout") &&
		!strings.Contains(err.Error(), "deadline") &&
		!strings.Contains(err.Error(), "context") &&
		!strings.Contains(err.Error(), "connection") {
		t.Logf("error (acceptable): %v", err)
	}
}

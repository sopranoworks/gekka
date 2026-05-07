/*
 * cluster_persistence_proxy.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// PersistenceProxyTransport is the cluster-backed persistence.ProxyTransport
// used by HOCON-wired off-mode ProxyJournal / ProxySnapshotStore
// instances.  Each Ask serialises the request as a raw byte payload,
// routes it via Cluster.Ask to the supplied target Pekko URI, and
// returns the reply payload.  The transport intentionally treats the
// request bytes as opaque: it does not register any serializer for
// rpcRequest / rpcResponse, so the byte path matches what the unit
// tests exercise with the in-process transport.
type PersistenceProxyTransport struct {
	cluster *Cluster
}

// NewPersistenceProxyTransport returns a ProxyTransport that routes
// off-mode persistence-proxy requests through cluster's Artery layer.
// Production wiring happens automatically inside NewCluster, so end
// users rarely call this directly; tests use it to install a
// cluster-backed transport explicitly.
func NewPersistenceProxyTransport(cluster *Cluster) *PersistenceProxyTransport {
	return &PersistenceProxyTransport{cluster: cluster}
}

// Ask satisfies persistence.ProxyTransport.
func (t *PersistenceProxyTransport) Ask(ctx context.Context, targetAddress string, payload []byte, timeout time.Duration) ([]byte, error) {
	if t == nil || t.cluster == nil {
		return nil, fmt.Errorf("persistence proxy transport: cluster not initialised")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	reply, err := t.cluster.Ask(ctx, targetAddress, payload)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, fmt.Errorf("persistence proxy transport: empty reply")
	}
	return reply.Payload, nil
}

// ServePersistenceProxyJournal registers an actor on cluster at the
// well-known path persistence.RemoteJournalPathSuffix that decodes
// inbound RPC requests, dispatches them to journal, and sends the
// gob-encoded reply back through the Artery sender ref.  This is the
// server-side counterpart to off-mode ProxyJournal: a remote
// persistent actor uses its proxy + transport to talk to the journal
// hosted here.  Calling twice replaces the previous handler.
func ServePersistenceProxyJournal(cluster *Cluster, journal persistence.Journal) {
	server := newJournalProxyServer(journal)
	actor.Start(server)
	cluster.RegisterActor(persistence.RemoteJournalPathSuffix, server)
}

// ServePersistenceProxySnapshot is the snapshot-store equivalent of
// ServePersistenceProxyJournal.  It hosts the supplied SnapshotStore
// behind the well-known path persistence.RemoteSnapshotPathSuffix.
func ServePersistenceProxySnapshot(cluster *Cluster, store persistence.SnapshotStore) {
	server := newSnapshotProxyServer(store)
	actor.Start(server)
	cluster.RegisterActor(persistence.RemoteSnapshotPathSuffix, server)
}

// ── Server actors ─────────────────────────────────────────────────────────────

type journalProxyServer struct {
	actor.BaseActor
	journal persistence.Journal
}

func newJournalProxyServer(j persistence.Journal) *journalProxyServer {
	return &journalProxyServer{
		BaseActor: actor.NewBaseActor(),
		journal:   j,
	}
}

func (s *journalProxyServer) Receive(msg any) {
	payload, ok := extractRPCPayload(msg)
	if !ok {
		return
	}
	reply, err := persistence.ServeJournalRequest(context.Background(), s.journal, payload)
	if err != nil {
		return
	}
	sender := s.Sender()
	if sender == nil || sender.Path() == "" {
		return
	}
	sender.Tell(reply)
}

type snapshotProxyServer struct {
	actor.BaseActor
	store persistence.SnapshotStore
}

func newSnapshotProxyServer(s persistence.SnapshotStore) *snapshotProxyServer {
	return &snapshotProxyServer{
		BaseActor: actor.NewBaseActor(),
		store:     s,
	}
}

func (s *snapshotProxyServer) Receive(msg any) {
	payload, ok := extractRPCPayload(msg)
	if !ok {
		return
	}
	reply, err := persistence.ServeSnapshotRequest(context.Background(), s.store, payload)
	if err != nil {
		return
	}
	sender := s.Sender()
	if sender == nil || sender.Path() == "" {
		return
	}
	sender.Tell(reply)
}

// extractRPCPayload pulls the raw byte payload from whatever the
// Artery dispatch handed the actor.  When the cluster's serialization
// registry resolves the inbound serializer (id 4 / ByteArray), the
// envelope arrives as a []byte; for unresolved serializers it stays
// wrapped in *IncomingMessage.  Both shapes are valid for the proxy
// transport.
func extractRPCPayload(msg any) ([]byte, bool) {
	switch v := msg.(type) {
	case []byte:
		return v, true
	case *IncomingMessage:
		return v.Payload, true
	default:
		return nil, false
	}
}

// ── Auto-wiring ───────────────────────────────────────────────────────────────

// installedProxyTransports tracks the cluster instances whose proxy
// transports are currently registered as the persistence-package
// global.  Cluster.Shutdown uses this to clear its own registration
// without trampling on a transport another test owns.
var installedProxyTransports atomic.Pointer[*PersistenceProxyTransport]

// installPersistenceProxyTransport registers cluster's transport with
// the persistence package so HOCON-wired off-mode proxies created
// after NewCluster returns can find it.  Call from NewCluster.
func installPersistenceProxyTransport(cluster *Cluster) {
	t := NewPersistenceProxyTransport(cluster)
	persistence.RegisterProxyTransport(t)
	installedProxyTransports.Store(&t)
}

// uninstallPersistenceProxyTransport clears the persistence-package
// global if (and only if) it still points at this cluster's transport.
// Called from Cluster.Shutdown so a test that creates and tears down
// a cluster does not leave behind a dangling transport for the next
// test in the same process.
func uninstallPersistenceProxyTransport(cluster *Cluster) {
	prev := installedProxyTransports.Load()
	if prev == nil || *prev == nil {
		return
	}
	if (*prev).cluster != cluster {
		return
	}
	persistence.RegisterProxyTransport(nil)
	installedProxyTransports.CompareAndSwap(prev, nil)
}

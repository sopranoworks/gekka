/*
 * received_quarantined_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestReceivedQuarantined_DoesNotPermanentlyBlacklistSender pins the
// recovery contract for inbound Quarantined control messages.
//
// Pekko sends Quarantined for HARMLESS association-scoped events too — a
// boot-race handshake timeout makes ClusterRemoteWatcher quarantine the
// peer's UID with "Cluster member removed, new incarnation joined"
// (harmless=true), after which Pekko RE-HANDSHAKES to recover. gekka's
// prior behaviour registered the SENDER's UID in the permanent quarantine
// registry, so handleHandshakeReq refused every recovery attempt — a
// transient event became a permanent transport partition between two Up
// members (observed 2026-07-13, showcase run gate2fix2-v2: s4↔g3 RST
// loop → FD unreachable → SBR downed the cluster).
//
// Contract: the association is torn down (QUARANTINED, conn closed), but
// the sender's UID stays OUT of the permanent registry so a fresh
// HandshakeReq from the same (healthy, same-incarnation) peer succeeds.
func TestReceivedQuarantined_DoesNotPermanentlyBlacklistSender(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2543, "S"), 1)

	peer := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("127.0.0.1", 2554, "S"),
		Uid:     proto.Uint64(8511776205773668081),
	}
	self := &gproto_remote.UniqueAddress{
		Address: nm.LocalAddr,
		Uid:     proto.Uint64(nm.localUid),
	}

	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     INBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 4),
		streamId: 1,
		lastSeen: time.Now(),
	}
	assoc.remote.Store(peer)

	quarPayload, err := proto.Marshal(&gproto_remote.Quarantined{From: peer, To: self})
	if err != nil {
		t.Fatalf("marshal Quarantined: %v", err)
	}
	meta := &ArteryMetadata{
		SerializerId:    actor.ArteryInternalSerializerID,
		MessageManifest: []byte("a"),
		Payload:         quarPayload,
	}
	if err := assoc.handleControlMessage(context.Background(), meta); err != nil {
		t.Fatalf("handleControlMessage: %v", err)
	}

	// The association itself is torn down...
	if assoc.GetState() != QUARANTINED {
		t.Errorf("association state = %v, want QUARANTINED", assoc.GetState())
	}
	// ...but the sender's UID must NOT be permanently blacklisted.
	if nm.IsQuarantined(peer.GetUid()) {
		t.Fatal("received Quarantined must not permanently blacklist the sender's UID — recovery re-handshakes would be refused forever")
	}

	// A fresh HandshakeReq from the same peer (same UID) must be accepted.
	fresh := &GekkaAssociation{
		state:    INITIATED,
		role:     INBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 4),
		streamId: 1,
		lastSeen: time.Now(),
	}
	fresh.remote.Store(&gproto_remote.UniqueAddress{Address: peer.Address, Uid: proto.Uint64(0)})
	req := &gproto_remote.HandshakeReq{From: peer, To: nm.LocalAddr}
	if err := fresh.handleHandshakeReq(req); err != nil {
		t.Fatalf("recovery HandshakeReq must be accepted, got: %v", err)
	}
	if fresh.GetState() != ASSOCIATED {
		t.Errorf("recovery association state = %v, want ASSOCIATED", fresh.GetState())
	}
}

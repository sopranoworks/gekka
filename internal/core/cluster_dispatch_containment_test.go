/*
 * cluster_dispatch_containment_test.go
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
	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestDispatch_ClusterMessageFailureDoesNotKillConnection pins the
// containment contract: an inbound cluster message whose processing fails
// (decode error here; a reply hitting a full outbox is the production
// case) must NOT propagate an error out of the frame handler, because
// tcpArteryReadLoop treats any handler error as fatal and closes the
// inbound connection. During the 2026-07-13 showcase collapse this
// converted one association's full outbox into a 1 Hz kill-reconnect loop
// of the peer's healthy inbound control stream.
func TestDispatch_ClusterMessageFailureDoesNotKillConnection(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("S"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(1),
	}
	cm := cluster.NewClusterManager(local, func(context.Context, string, any) error { return nil })
	nm.SetClusterManager(cm)

	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.2.1", 2551, "Remote"),
		Uid:     proto.Uint64(4242),
	}
	assoc := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     INBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		streamId: 1,
		lastSeen: time.Now(),
	}
	assoc.remote.Store(remote)

	// A GossipEnvelope manifest with a garbage payload fails inside
	// HandleIncomingClusterMessage. Pre-containment, dispatch returned
	// that error and the read loop tore the connection down.
	meta := &ArteryMetadata{
		Sender:          &gproto_remote.ActorRefData{Path: proto.String("pekko://Remote@10.0.2.1:2551/system/cluster/core/daemon")},
		Recipient:       &gproto_remote.ActorRefData{Path: proto.String("pekko://S@127.0.0.1:2552/system/cluster/core/daemon")},
		SerializerId:    actor.ClusterSerializerID,
		MessageManifest: []byte("GE"),
		Payload:         []byte{0xde, 0xad, 0xbe, 0xef},
	}

	// Sanity: the underlying handler really does fail on this input —
	// otherwise this test would pass vacuously.
	if err := cm.HandleIncomingClusterMessage(context.Background(), meta.Payload, "GE", ToClusterUniqueAddress(remote), meta.Sender.GetPath()); err == nil {
		t.Fatal("expected HandleIncomingClusterMessage to fail on garbage GossipEnvelope payload")
	}

	if err := assoc.dispatch(context.Background(), meta); err != nil {
		t.Fatalf("dispatch must contain cluster-message failures (connection kept), got: %v", err)
	}
}

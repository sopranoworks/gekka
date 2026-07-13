/*
 * handshake_sibling_won_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"testing"
	"time"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestHasOtherAssociatedTo covers the classifier behind the handshake
// retry loop's WARN-vs-Debug decision: a handshake deadline on an
// association whose sibling (same remote host:port + streamId) is already
// ASSOCIATED is symmetric-handshake-race fallout, not a reachability
// problem.
func TestHasOtherAssociatedTo(t *testing.T) {
	nm := NewNodeManager(lifecycleTestAddr("127.0.0.1", 2552, "S"), 1)

	remote := &gproto_remote.UniqueAddress{
		Address: lifecycleTestAddr("10.0.3.1", 2551, "Remote"),
		Uid:     proto.Uint64(5151),
	}

	winner := &GekkaAssociation{
		state:    ASSOCIATED,
		role:     INBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		streamId: 1,
		lastSeen: time.Now(),
	}
	winner.remote.Store(remote)
	nm.RegisterAssociation(remote, winner)

	loser := &GekkaAssociation{
		state:    INITIATED,
		role:     OUTBOUND,
		nodeMgr:  nm,
		localUid: nm.localUid,
		outbox:   make(chan []byte, 1),
		streamId: 1,
	}
	loser.remote.Store(remote)

	if !nm.hasOtherAssociatedTo(loser, "10.0.3.1", 2551, 1) {
		t.Error("expected the loser to see the winner's established association")
	}
	// The established association itself must not count as its own sibling.
	if nm.hasOtherAssociatedTo(winner, "10.0.3.1", 2551, 1) {
		t.Error("an association must not count itself")
	}
	// Different streamId does not satisfy the check.
	if nm.hasOtherAssociatedTo(loser, "10.0.3.1", 2551, 2) {
		t.Error("streamId=2 has no established association")
	}
	// Unrelated remote does not satisfy the check.
	if nm.hasOtherAssociatedTo(loser, "10.0.3.9", 2551, 1) {
		t.Error("unrelated remote must not match")
	}
}

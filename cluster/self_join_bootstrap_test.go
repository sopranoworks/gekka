/*
 * self_join_bootstrap_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// TestSelfJoin_JoinerMustNotStealLeadershipWithUpNumber pins the
// TestGoSeed_FailureRecovery flake — a real bug surfaced under load that
// caused the live diag's Joining-stuck convergence too.
//
// Root cause: addMemberToGossipLocked (cluster_manager.go:1254 / :1274)
// assigned `UpNumber = len(State.Members) + 1` to a newly-Joining member.
// Per Pekko's MembershipState.leaderOrdering, non-Up members are treated as
// having UpNumber=MaxValue (sentinel) for leader-selection ordering — i.e.
// UpNumber on a Joining member should remain 0 (the unassigned default)
// until promotion to Up.
//
// When gekka is its own seed and a remote node joins before gekka's first
// leader-action tick fires, addMember writes the joiner with UpNumber=2.
// Subsequent DetermineLeader sees the local node with UpNumber=0 (sentinel)
// vs the joiner with UpNumber=2 (real) and picks the joiner as leader
// (smaller UpNumber wins). Gekka loses leadership → cannot promote itself
// → stays Joining indefinitely → test times out.
//
// The fix is to set UpNumber=0 at Join. This test pins the contract.
func TestSelfJoin_JoinerMustNotStealLeadershipWithUpNumber(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("ClusterSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2550),
		},
		Uid:  proto.Uint32(0xCAFE),
		Uid2: proto.Uint32(0),
	}
	router := func(ctx context.Context, path string, msg any) error { return nil }
	cm := NewClusterManager(local, router)

	// Production Cluster.Join(self) path — a self-join does NOT arm the
	// remote-join pre-Welcome gate, so leader actions stay enabled.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selfHost := local.GetAddress().GetHostname()
	selfPort := local.GetAddress().GetPort()
	_ = cm.JoinCluster(ctx, selfHost, selfPort)

	// Remote peer (Scala, higher port) joins before gekka's first
	// leader-action tick — the failing race.
	peerAddr := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("ClusterSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(58142),
		},
		Uid:  proto.Uint32(0xBEEF),
		Uid2: proto.Uint32(0),
	}
	cm.Mu.Lock()
	cm.addMemberToGossipLocked(peerAddr, nil, "")
	cm.Mu.Unlock()

	// Newly-Joining peer must have UpNumber=0 — Pekko semantics require
	// UpNumber be set only at promotion to Up.
	cm.Mu.RLock()
	if len(cm.State.Members) != 2 {
		cm.Mu.RUnlock()
		t.Fatalf("expected 2 members, got %d", len(cm.State.Members))
	}
	peerMember := cm.State.Members[1]
	cm.Mu.RUnlock()
	if peerMember.GetUpNumber() != 0 {
		t.Errorf("Joining peer UpNumber = %d, want 0 — assigning a real UpNumber to a Joining member breaks DetermineLeader",
			peerMember.GetUpNumber())
	}

	// With both members Joining (no Up/Leaving yet), DetermineLeader's
	// bootstrap fallback orders by address (host → port → uid) and must
	// pick the lower-port local node as leader.
	leader := cm.DetermineLeader()
	if leader == nil {
		t.Fatal("DetermineLeader returned nil — bootstrap fallback broken")
	}
	if leader.GetAddress().GetPort() != selfPort {
		t.Errorf("leader port = %d, want %d (local) — joiner stole leadership via non-zero UpNumber",
			leader.GetAddress().GetPort(), selfPort)
	}

	// Leader-action must promote local to Up. With local-as-leader (a
	// self-join keeps leader actions enabled) and canBootstrap closed
	// (two members in view), the convergence branch must succeed because
	// no Up/Leaving members exist yet (Seen check is trivially satisfied).
	cm.performLeaderActions()

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	upCount := 0
	for _, m := range cm.State.Members {
		if m.GetStatus() == gproto_cluster.MemberStatus_Up {
			upCount++
		}
	}
	if upCount != 2 {
		t.Errorf("expected 2 Up members after leader-action, got %d — Joining-stuck convergence", upCount)
		for i, m := range cm.State.Members {
			ua := cm.State.AllAddresses[m.GetAddressIndex()]
			t.Logf("  member[%d] host=%s port=%d status=%s upNumber=%d",
				i, ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(),
				m.GetStatus(), m.GetUpNumber())
		}
	}
}

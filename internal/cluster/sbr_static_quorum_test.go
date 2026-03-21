/*
 * sbr_static_quorum_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"fmt"
	"testing"
)

// makeMembers builds a slice of n Members with sequential addresses for testing.
func makeMembers(n int) []Member {
	members := make([]Member, n)
	for i := range members {
		members[i] = Member{Host: fmt.Sprintf("10.0.0.%d", i+1), Port: 2551}
	}
	return members
}

// TestStaticQuorum_5NodeCluster_3Unreachable verifies that a 2-node partition
// (out of 5) returns Down when QuorumSize=3.
func TestStaticQuorum_5NodeCluster_3Unreachable(t *testing.T) {
	s := &StaticQuorumStrategy{QuorumSize: 3}

	all := makeMembers(5)          // 5-node cluster
	unreachable := all[2:]         // last 3 are unreachable → local side sees 2 reachable

	action := s.Decide(all, unreachable)
	if action != Down {
		t.Errorf("5-node cluster, 3 unreachable, quorum=3: expected Down, got %v", action)
	}
}

// TestStaticQuorum_5NodeCluster_2Unreachable verifies that a 3-node partition
// (out of 5) returns Keep when QuorumSize=3.
func TestStaticQuorum_5NodeCluster_2Unreachable(t *testing.T) {
	s := &StaticQuorumStrategy{QuorumSize: 3}

	all := makeMembers(5)          // 5-node cluster
	unreachable := all[3:]         // last 2 are unreachable → local side sees 3 reachable

	action := s.Decide(all, unreachable)
	if action != Keep {
		t.Errorf("5-node cluster, 2 unreachable, quorum=3: expected Keep, got %v", action)
	}
}

// TestStaticQuorum_3NodeCluster_1Unreachable verifies that a 2-node partition
// (out of 3) returns Keep when QuorumSize=2.
func TestStaticQuorum_3NodeCluster_1Unreachable(t *testing.T) {
	s := &StaticQuorumStrategy{QuorumSize: 2}

	all := makeMembers(3)          // 3-node cluster
	unreachable := all[2:]         // last 1 is unreachable → local side sees 2 reachable

	action := s.Decide(all, unreachable)
	if action != Keep {
		t.Errorf("3-node cluster, 1 unreachable, quorum=2: expected Keep, got %v", action)
	}
}

// TestStaticQuorum_ClusterTooSmall verifies that a cluster smaller than
// QuorumSize always returns Down regardless of reachability.
func TestStaticQuorum_ClusterTooSmall(t *testing.T) {
	s := &StaticQuorumStrategy{QuorumSize: 5}

	all := makeMembers(3)       // only 3 members — can never reach quorum of 5
	unreachable := []Member{}   // even with all reachable, total < QuorumSize

	action := s.Decide(all, unreachable)
	if action != Down {
		t.Errorf("cluster size 3 < quorum 5: expected Down, got %v", action)
	}
}

// TestStaticQuorum_ExactlyAtQuorum verifies the boundary: reachable == QuorumSize → Keep.
func TestStaticQuorum_ExactlyAtQuorum(t *testing.T) {
	s := &StaticQuorumStrategy{QuorumSize: 3}

	all := makeMembers(5)
	unreachable := all[3:4] // 1 unreachable → 4 reachable; well above quorum
	action := s.Decide(all, unreachable)
	if action != Keep {
		t.Errorf("4 reachable of 5, quorum=3: expected Keep, got %v", action)
	}

	// Now exactly at boundary: 3 reachable, 2 unreachable
	unreachable2 := all[3:] // 2 unreachable → 3 reachable == QuorumSize
	action2 := s.Decide(all, unreachable2)
	if action2 != Keep {
		t.Errorf("3 reachable of 5, quorum=3: expected Keep at boundary, got %v", action2)
	}
}

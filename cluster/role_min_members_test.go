/*
 * role_min_members_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

func TestMeetsRoleMinNrOfMembers_NoConfig(t *testing.T) {
	cm := &ClusterManager{
		State: &gproto_cluster.Gossip{},
	}
	// No RoleMinNrOfMembers configured — should always pass.
	if !cm.meetsRoleMinNrOfMembersLocked() {
		t.Fatal("expected true when RoleMinNrOfMembers is nil")
	}
}

func TestMeetsRoleMinNrOfMembers_Satisfied(t *testing.T) {
	cm := &ClusterManager{
		RoleMinNrOfMembers: map[string]int{
			"backend": 2,
		},
		State: &gproto_cluster.Gossip{
			AllRoles: []string{"dc-default", "backend"},
			Members: []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1}, // dc-default, backend
				},
				{
					AddressIndex: proto.Int32(1),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1}, // dc-default, backend
				},
			},
		},
	}
	if !cm.meetsRoleMinNrOfMembersLocked() {
		t.Fatal("expected true: 2 backend members meets min 2")
	}
}

func TestMeetsRoleMinNrOfMembers_NotSatisfied(t *testing.T) {
	cm := &ClusterManager{
		RoleMinNrOfMembers: map[string]int{
			"backend": 3,
		},
		State: &gproto_cluster.Gossip{
			AllRoles: []string{"dc-default", "backend"},
			Members: []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1},
				},
				{
					AddressIndex: proto.Int32(1),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1},
				},
			},
		},
	}
	if cm.meetsRoleMinNrOfMembersLocked() {
		t.Fatal("expected false: 2 backend members does not meet min 3")
	}
}

func TestMeetsRoleMinNrOfMembers_MultiRole(t *testing.T) {
	cm := &ClusterManager{
		RoleMinNrOfMembers: map[string]int{
			"backend":  2,
			"frontend": 1,
		},
		State: &gproto_cluster.Gossip{
			AllRoles: []string{"dc-default", "backend", "frontend"},
			Members: []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1}, // dc-default, backend
				},
				{
					AddressIndex: proto.Int32(1),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1}, // dc-default, backend
				},
				{
					AddressIndex: proto.Int32(2),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 2}, // dc-default, frontend
				},
			},
		},
	}
	if !cm.meetsRoleMinNrOfMembersLocked() {
		t.Fatal("expected true: 2 backend and 1 frontend meets both minimums")
	}
}

func TestMeetsRoleMinNrOfMembers_OneRoleMissing(t *testing.T) {
	cm := &ClusterManager{
		RoleMinNrOfMembers: map[string]int{
			"backend":  2,
			"frontend": 2, // need 2 but only have 1
		},
		State: &gproto_cluster.Gossip{
			AllRoles: []string{"dc-default", "backend", "frontend"},
			Members: []*gproto_cluster.Member{
				{
					AddressIndex: proto.Int32(0),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1},
				},
				{
					AddressIndex: proto.Int32(1),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 1},
				},
				{
					AddressIndex: proto.Int32(2),
					Status:       gproto_cluster.MemberStatus_Up.Enum(),
					RolesIndexes: []int32{0, 2}, // only 1 frontend
				},
			},
		},
	}
	if cm.meetsRoleMinNrOfMembersLocked() {
		t.Fatal("expected false: only 1 frontend member, need 2")
	}
}

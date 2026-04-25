/*
 * cluster/client/receptionist_internal_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package client

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// nopRouter satisfies cluster.Router with no-op delivery.
type nopRouter struct{}

func (nopRouter) Send(_ context.Context, _ string, _ any) error                { return nil }
func (nopRouter) SendWithSender(_ context.Context, _, _ string, _ any) error { return nil }

// makeMember constructs a Member referencing a freshly-appended address slot.
// It mutates state in place and returns nothing; the caller can read back via
// state.Members and state.AllAddresses.
func appendUpMember(state *gproto_cluster.Gossip, host string, port uint32, upNumber int32) {
	addr := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String(host),
			Port:     proto.Uint32(port),
		},
		Uid:  proto.Uint32(uint32(upNumber + 100)),
		Uid2: proto.Uint32(0),
	}
	idx := int32(len(state.AllAddresses))
	state.AllAddresses = append(state.AllAddresses, addr)
	state.Members = append(state.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(idx),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(upNumber),
	})
}

// receptionistTestCM builds a ClusterManager with `extra` Up peers in addition
// to the local node, sufficient to drive collectContactPaths against varying
// NumberOfContacts caps.
func receptionistTestCM(extra int) *cluster.ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2551),
		},
		Uid:  proto.Uint32(1),
		Uid2: proto.Uint32(0),
	}
	cm := cluster.NewClusterManager(local, func(_ context.Context, _ string, _ any) error { return nil })
	state := cm.GetState()
	state.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	state.Members[0].UpNumber = proto.Int32(1)
	for i := 0; i < extra; i++ {
		appendUpMember(state, "127.0.0.1", uint32(2552+i), int32(2+i))
	}
	return cm
}

// TestReceptionist_CollectContactPaths_HonorsConfiguredCap verifies that
// collectContactPaths caps the returned slice at cfg.NumberOfContacts when
// the cluster has more members than the cap. This is the round-2 session 07
// behavior assertion: HOCON receptionist.number-of-contacts → ContactList size.
func TestReceptionist_CollectContactPaths_HonorsConfiguredCap(t *testing.T) {
	// Cluster has 1 (local) + 5 (extras) = 6 Up members.
	cm := receptionistTestCM(5)

	cases := []struct {
		name string
		cap  int
		want int
	}{
		{"cap-below-cluster-size", 2, 2},
		{"cap-equal-cluster-size", 6, 6},
		{"cap-above-cluster-size", 10, 6},
		{"default-pekko-cap", 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ReceptionistConfig{
				Name:                     "receptionist",
				NumberOfContacts:         tc.cap,
				HeartbeatInterval:        2 * time.Second,
				AcceptableHeartbeatPause: 13 * time.Second,
			}
			r := NewClusterReceptionist(cm, cfg, nopRouter{})
			got := r.collectContactPaths()
			if len(got) != tc.want {
				t.Errorf("len(collectContactPaths()) = %d, want %d (cap=%d, members=6)",
					len(got), tc.want, tc.cap)
			}
		})
	}
}

// TestReceptionist_CollectContactPaths_UsesConfiguredName verifies that the
// receptionist actor name from cfg.Name is reflected in the returned contact
// paths (the trailing /system/<name> segment).
func TestReceptionist_CollectContactPaths_UsesConfiguredName(t *testing.T) {
	cm := receptionistTestCM(0) // only local
	cfg := ReceptionistConfig{
		Name:                     "frontDesk",
		NumberOfContacts:         3,
		HeartbeatInterval:        2 * time.Second,
		AcceptableHeartbeatPause: 13 * time.Second,
	}
	r := NewClusterReceptionist(cm, cfg, nopRouter{})
	paths := r.collectContactPaths()
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1", len(paths))
	}
	want := "pekko://TestSystem@127.0.0.1:2551/system/frontDesk"
	if paths[0] != want {
		t.Errorf("paths[0] = %q, want %q", paths[0], want)
	}
}

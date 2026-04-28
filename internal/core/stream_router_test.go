/*
 * stream_router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestLargeMessageRouter_GlobMatching exercises every supported pattern form
// against a representative set of recipient paths, in URI and bare-path form.
func TestLargeMessageRouter_GlobMatching(t *testing.T) {
	cases := []struct {
		name     string
		patterns []string
		input    string
		want     bool
	}{
		// Empty router never matches.
		{"empty", nil, "/user/anything", false},
		{"empty-uri", []string{}, "pekko://Sys@host:2552/user/anything", false},

		// Exact paths.
		{"exact-match", []string{"/user/large-1"}, "/user/large-1", true},
		{"exact-no-match", []string{"/user/large-1"}, "/user/large-2", false},
		{"exact-uri-match", []string{"/user/large-1"}, "pekko://Sys@h:2552/user/large-1", true},

		// Trailing wildcard within a single segment.
		{"trailing-glob-match", []string{"/user/large-*"}, "/user/large-99", true},
		{"trailing-glob-no-match-extra-seg", []string{"/user/large-*"}, "/user/large-99/child", false},
		{"trailing-glob-no-match-prefix", []string{"/user/large-*"}, "/user/normal-1", false},

		// Per-segment "*".
		{"single-star-match", []string{"/user/*/big"}, "/user/foo/big", true},
		{"single-star-no-match-too-deep", []string{"/user/*/big"}, "/user/foo/bar/big", false},

		// Tail "**".
		{"tail-star-match-zero", []string{"/user/large/**"}, "/user/large", false}, // ** requires at least matching prefix len
		{"tail-star-match-one", []string{"/user/large/**"}, "/user/large/x", true},
		{"tail-star-match-deep", []string{"/user/large/**"}, "/user/large/a/b/c", true},

		// Glob in middle of segment.
		{"prefix-suffix-glob", []string{"/user/big-*-bin"}, "/user/big-payload-bin", true},
		{"prefix-suffix-glob-no-match", []string{"/user/big-*-bin"}, "/user/big-payload-img", false},

		// Multiple patterns: any match wins.
		{"multi-first-matches", []string{"/user/large-*", "/svc/blob"}, "/user/large-7", true},
		{"multi-second-matches", []string{"/user/large-*", "/svc/blob"}, "/svc/blob", true},
		{"multi-none-matches", []string{"/user/large-*", "/svc/blob"}, "/user/normal", false},

		// Whitespace-only patterns are skipped.
		{"whitespace-pattern-skipped", []string{"   ", "/user/large-1"}, "/user/large-1", true},
		{"whitespace-pattern-only", []string{"   "}, "/user/large-1", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewLargeMessageRouter(tc.patterns)
			if got := r.IsLarge(tc.input); got != tc.want {
				t.Errorf("IsLarge(%q) with patterns %v = %v, want %v", tc.input, tc.patterns, got, tc.want)
			}
		})
	}
}

// TestLargeMessageRouter_NilSafe verifies a nil router and an empty
// recipient string never panic and always return false.
func TestLargeMessageRouter_NilSafe(t *testing.T) {
	var r *LargeMessageRouter
	if r.IsLarge("/user/anything") {
		t.Fatalf("nil router should never report large")
	}
	r2 := NewLargeMessageRouter([]string{"/user/large-*"})
	if r2.IsLarge("") {
		t.Fatalf("empty recipient should never match")
	}
	if r2.IsLarge("not-a-uri-no-slash") {
		t.Fatalf("recipient without path segments should not match")
	}
}

// TestNodeManager_LargeMessageDestinations_Wiring verifies that the router
// installed by SetLargeMessageDestinations is reachable through
// IsLargeRecipient — i.e. the cluster-spawn pipeline can consult routing
// without owning a reference to the LargeMessageRouter directly.
func TestNodeManager_LargeMessageDestinations_Wiring(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	// No destinations configured → no recipient matches.
	if nm.IsLargeRecipient("/user/large-1") {
		t.Fatalf("default NodeManager should treat all recipients as non-large")
	}

	nm.SetLargeMessageDestinations([]string{"/user/large-*", "/svc/blob"})

	if !nm.IsLargeRecipient("/user/large-99") {
		t.Errorf("expected /user/large-99 to route as large after SetLargeMessageDestinations")
	}
	if !nm.IsLargeRecipient("pekko://Sys@h:2552/svc/blob") {
		t.Errorf("expected URI form /svc/blob to route as large")
	}
	if nm.IsLargeRecipient("/user/normal") {
		t.Errorf("/user/normal should not route as large")
	}

	// Replacement clears prior matches.
	nm.SetLargeMessageDestinations(nil)
	if nm.IsLargeRecipient("/user/large-99") {
		t.Errorf("clearing destinations should make all recipients non-large again")
	}
}

// TestAssociation_OutboxFor_RoutesLargeUserMessages verifies that
// outboxFor returns the dedicated large-stream outbox when the recipient
// matches a configured glob and the serializer is a user serializer; and
// returns the ordinary outbox otherwise.  Cluster/Artery-internal control
// traffic must never be routed to the large stream regardless of
// destination match.
func TestAssociation_OutboxFor_RoutesLargeUserMessages(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.SetLargeMessageDestinations([]string{"/user/large-*"})

	// Simulate a UDP-style association: ordinary + large outboxes both present.
	ord := make(chan []byte, 1)
	large := make(chan []byte, 1)
	primary := make(chan []byte, 1)
	assoc := &GekkaAssociation{
		nodeMgr:           nm,
		outbox:            primary,
		udpOrdinaryOutbox: ord,
		udpLargeOutbox:    large,
	}

	const userSerializerID = 4 // ByteArraySerializer (representative user serializer)

	// Large recipient + user serializer → large outbox.
	if got := assoc.outboxFor("/user/large-1", userSerializerID); got != large {
		t.Errorf("user message to large recipient should select large outbox")
	}
	// Non-large recipient + user serializer → ordinary outbox.
	if got := assoc.outboxFor("/user/normal", userSerializerID); got != ord {
		t.Errorf("user message to normal recipient should select ordinary outbox")
	}
	// Cluster control traffic → primary outbox even when path matches.
	if got := assoc.outboxFor("/user/large-1", ClusterSerializerID); got != primary {
		t.Errorf("cluster control traffic must never route to large outbox")
	}
	// Artery-internal traffic → primary outbox.
	if got := assoc.outboxFor("/user/large-1", ArteryInternalSerializerID); got != primary {
		t.Errorf("artery-internal traffic must never route to large outbox")
	}

	// TCP-style association (no UDP outboxes): outboxFor must fall through to
	// the primary outbox for every recipient — the large stream is not yet
	// open over TCP (deferred to session 30).
	tcpAssoc := &GekkaAssociation{nodeMgr: nm, outbox: primary}
	if got := tcpAssoc.outboxFor("/user/large-1", userSerializerID); got != primary {
		t.Errorf("TCP association without large outbox should fall through to primary")
	}
}

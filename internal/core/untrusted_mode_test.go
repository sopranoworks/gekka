/*
 * untrusted_mode_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"context"
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

// TestIsPossiblyHarmfulManifest verifies the (serializerId, manifest) tuples
// gekka treats as PossiblyHarmful for untrusted-mode enforcement: PoisonPill
// and Kill from MiscMessageSerializer plus the SystemMessage envelope from
// ArteryInternalSerializer (which carries DeathWatch Watch/Unwatch).
func TestIsPossiblyHarmfulManifest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sid      int32
		manifest string
		want     bool
	}{
		{"poison pill", MiscMessageSerializerID, "P", true},
		{"kill", MiscMessageSerializerID, "K", true},
		{"system message", actor.ArteryInternalSerializerID, "SystemMessage", true},
		{"identify is benign", MiscMessageSerializerID, "A", false},
		{"actor identity is benign", MiscMessageSerializerID, "B", false},
		{"handshake req is benign", actor.ArteryInternalSerializerID, "a", false},
		{"byte array is benign", 4, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := IsPossiblyHarmfulManifest(tc.sid, tc.manifest)
			if got != tc.want {
				t.Errorf("IsPossiblyHarmfulManifest(%d, %q) = %v, want %v", tc.sid, tc.manifest, got, tc.want)
			}
		})
	}
}

// TestNodeManager_UntrustedModeDrop verifies that the gate is no-op when
// UntrustedMode is off and only fires for PossiblyHarmful tuples when on.
func TestNodeManager_UntrustedModeDrop(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	// Off: nothing dropped.
	if nm.untrustedModeDrop(MiscMessageSerializerID, "P") {
		t.Fatal("UntrustedMode=off should never drop")
	}
	if nm.untrustedModeDrop(actor.ArteryInternalSerializerID, "SystemMessage") {
		t.Fatal("UntrustedMode=off should never drop SystemMessage")
	}

	// On: PossiblyHarmful tuples are dropped, benign tuples pass.
	nm.UntrustedMode = true
	if !nm.untrustedModeDrop(MiscMessageSerializerID, "P") {
		t.Error("UntrustedMode=on must drop PoisonPill")
	}
	if !nm.untrustedModeDrop(MiscMessageSerializerID, "K") {
		t.Error("UntrustedMode=on must drop Kill")
	}
	if !nm.untrustedModeDrop(actor.ArteryInternalSerializerID, "SystemMessage") {
		t.Error("UntrustedMode=on must drop SystemMessage")
	}
	if nm.untrustedModeDrop(MiscMessageSerializerID, "A") {
		t.Error("UntrustedMode=on must NOT drop Identify")
	}
	if nm.untrustedModeDrop(4, "") {
		t.Error("UntrustedMode=on must NOT drop benign user payloads")
	}
}

// TestHandleSystemMessage_UntrustedModeDrops verifies that when UntrustedMode
// is on, handleSystemMessage returns nil and never invokes the
// SystemMessageCallback. We deliberately feed an unparseable payload: under
// untrusted-mode the handler must early-return *before* attempting to parse
// the SystemMessageEnvelope. If the drop did not happen the parser would
// surface an error (proving the gate is correctly placed). The off-mode
// behaviour is already exercised by the existing handshake/cluster tests
// that drive valid SystemMessageEnvelope payloads end-to-end.
func TestHandleSystemMessage_UntrustedModeDrops(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.UntrustedMode = true

	called := false
	nm.SystemMessageCallback = func(_ *gproto_remote.UniqueAddress, _ *gproto_remote.SystemMessageEnvelope, _ *gproto_remote.SystemMessage) error {
		called = true
		return nil
	}

	assoc := &GekkaAssociation{nodeMgr: nm}
	meta := &ArteryMetadata{
		SerializerId:    actor.ArteryInternalSerializerID,
		MessageManifest: []byte("SystemMessage"),
		// Garbage bytes — would surface "proto: cannot parse" if the gate
		// failed to short-circuit before proto.Unmarshal.
		Payload: []byte{0xFF, 0xFE, 0xFD, 0x01, 0x02, 0x03},
	}
	if err := assoc.handleSystemMessage(meta); err != nil {
		t.Fatalf("handleSystemMessage returned error under untrusted-mode (gate did not fire before parser): %v", err)
	}
	if called {
		t.Fatal("SystemMessageCallback must not be invoked under untrusted-mode")
	}
}

// TestHandleUserMessage_UntrustedModeDropsPoisonPill verifies the user-message
// handler drops MiscMessageSerializer manifest "P" (PoisonPill) when
// UntrustedMode is on.
func TestHandleUserMessage_UntrustedModeDropsPoisonPill(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.UntrustedMode = true

	delivered := false
	nm.UserMessageCallback = func(_ context.Context, _ *ArteryMetadata) error { //nolint:revive // signature dictated by callback
		delivered = true
		return nil
	}

	assoc := &GekkaAssociation{nodeMgr: nm}
	meta := &ArteryMetadata{
		SerializerId:    MiscMessageSerializerID,
		MessageManifest: []byte("P"),
		Payload:         nil,
		Recipient:       &gproto_remote.ActorRefData{Path: proto.String("/user/foo")},
	}
	if err := assoc.handleUserMessage(meta); err != nil {
		t.Fatalf("handleUserMessage: %v", err)
	}
	if delivered {
		t.Fatal("PoisonPill must be dropped under untrusted-mode")
	}

	// Off: callback fires.
	nm.UntrustedMode = false
	if err := assoc.handleUserMessage(meta); err != nil {
		t.Fatalf("handleUserMessage: %v", err)
	}
	if !delivered {
		t.Fatal("PoisonPill must pass when untrusted-mode is off")
	}
}

// ── Session 32: trusted-selection-paths ─────────────────────────────────────

// TestNodeManager_IsTrustedSelectionPath verifies the allowlist matcher: an
// empty allowlist trusts nothing; entries match exactly on the bare actor
// path, with the URI scheme + authority stripped before comparison.
func TestNodeManager_IsTrustedSelectionPath(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)

	// Empty allowlist trusts nothing — matches Pekko's default behavior.
	if nm.IsTrustedSelectionPath("/user/echo") {
		t.Fatal("empty allowlist must not trust any path")
	}

	nm.TrustedSelectionPaths = []string{"/user/echo", "/user/api"}
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/user/echo", true},
		{"/user/api", true},
		{"pekko://Test@127.0.0.1:2552/user/echo", true}, // URI form is normalized
		{"/user/secret", false},                         // not in allowlist
		{"/user/echo/sub", false},                       // exact match required, not prefix
		{"/user", false},                                // parent of allowed entry is not allowed
		{"", false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			got := nm.IsTrustedSelectionPath(tc.path)
			if got != tc.want {
				t.Errorf("IsTrustedSelectionPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// buildSelectionEnvelopePayload produces an Artery SelectionEnvelope payload
// addressed at the supplied actor path. Used by the dispatch tests below to
// exercise the trusted-selection-paths gate end-to-end.
func buildSelectionEnvelopePayload(t *testing.T, path string, sid int32, manifest string) []byte {
	t.Helper()
	if !strings.HasPrefix(path, "/") {
		t.Fatalf("path must start with /: %q", path)
	}
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	pattern := make([]*gproto_remote.Selection, 0, len(segments))
	for _, seg := range segments {
		typ := gproto_remote.PatternType_CHILD_NAME
		pattern = append(pattern, &gproto_remote.Selection{
			Type:    &typ,
			Matcher: proto.String(seg),
		})
	}
	env := &gproto_remote.SelectionEnvelope{
		EnclosedMessage: []byte("hello"),
		SerializerId:    proto.Int32(sid),
		MessageManifest: []byte(manifest),
		Pattern:         pattern,
	}
	bytes, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal SelectionEnvelope: %v", err)
	}
	return bytes
}

// TestDispatch_SelectionEnvelope_UntrustedDropsNonAllowlisted verifies the S32
// gate: when UntrustedMode is on and the resolved selection path is NOT in
// TrustedSelectionPaths, the SelectionEnvelope is dropped before deserialize
// and the user-message callback is never invoked.
func TestDispatch_SelectionEnvelope_UntrustedDropsNonAllowlisted(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.UntrustedMode = true
	nm.TrustedSelectionPaths = []string{"/user/allowed"}

	called := false
	nm.UserMessageCallback = func(_ context.Context, _ *ArteryMetadata) error {
		called = true
		return nil
	}

	assoc := &GekkaAssociation{nodeMgr: nm}
	meta := &ArteryMetadata{
		SerializerId: 6, // MessageContainerSerializer
		Payload:      buildSelectionEnvelopePayload(t, "/user/forbidden", RawSerializerID, ""),
	}
	if err := assoc.dispatch(context.Background(), meta); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if called {
		t.Fatal("non-allowlisted selection must be dropped before user-message dispatch")
	}
}

// TestDispatch_SelectionEnvelope_UntrustedAllowsListedPath verifies that an
// allowlisted path passes the gate and reaches the user-message dispatcher
// (the inner ByteArray payload deserializes successfully and the callback
// fires).
func TestDispatch_SelectionEnvelope_UntrustedAllowsListedPath(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	nm.UntrustedMode = true
	nm.TrustedSelectionPaths = []string{"/user/allowed"}

	delivered := make(chan string, 1)
	nm.UserMessageCallback = func(_ context.Context, m *ArteryMetadata) error {
		if m.Recipient != nil {
			delivered <- m.Recipient.GetPath()
		} else {
			delivered <- ""
		}
		return nil
	}

	assoc := &GekkaAssociation{nodeMgr: nm}
	meta := &ArteryMetadata{
		SerializerId: 6,
		Payload:      buildSelectionEnvelopePayload(t, "/user/allowed", RawSerializerID, ""),
	}
	if err := assoc.dispatch(context.Background(), meta); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	select {
	case got := <-delivered:
		if got != "/user/allowed" {
			t.Errorf("delivered recipient = %q, want %q", got, "/user/allowed")
		}
	default:
		t.Fatal("allowlisted selection must reach user-message dispatcher")
	}
}

// TestDispatch_SelectionEnvelope_TrustedModeIgnoresAllowlist verifies that
// when UntrustedMode is off the allowlist is not consulted — selections to
// any path proceed to user-message dispatch (Pekko default behavior).
func TestDispatch_SelectionEnvelope_TrustedModeIgnoresAllowlist(t *testing.T) {
	nm := NewNodeManager(&gproto_remote.Address{
		Protocol: proto.String("pekko"),
		System:   proto.String("Test"),
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(0),
	}, 1)
	// UntrustedMode = false; allowlist intentionally empty.

	delivered := false
	nm.UserMessageCallback = func(_ context.Context, _ *ArteryMetadata) error {
		delivered = true
		return nil
	}

	assoc := &GekkaAssociation{nodeMgr: nm}
	meta := &ArteryMetadata{
		SerializerId: 6,
		Payload:      buildSelectionEnvelopePayload(t, "/user/anything", RawSerializerID, ""),
	}
	if err := assoc.dispatch(context.Background(), meta); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if !delivered {
		t.Fatal("untrusted-mode=off must not consult the allowlist")
	}
}

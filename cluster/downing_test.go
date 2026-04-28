/*
 * downing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// newTestClusterManager builds a minimal in-memory ClusterManager for
// the round-2 session 28 SBR-provider tests.  Mirrors the helper used
// in test/integration/self_healing_test.go but stays in-package so we
// don't drag the integration build tag into a unit-test file.
func newTestClusterManager(_ *testing.T) *ClusterManager {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(1),
	}
	return NewClusterManager(local, nil)
}

// Round-2 session 27 — F4 Downing: provider interface + registry tests.
// These tests stay at the cluster package level so the registry is
// covered without dragging in the gekka top-level wiring.

// fakeDowningProvider is a test double that records Start invocations.
// Lets us assert "the configured provider's Start was actually called"
// without spinning up a real cluster manager / SBR.
type fakeDowningProvider struct {
	name    string
	started atomic.Int32
}

func (p *fakeDowningProvider) Name() string                  { return p.name }
func (p *fakeDowningProvider) Start(_ context.Context) error { p.started.Add(1); return nil }

// TestNormalizeDowningProviderClass locks the FQCN-to-short-name table.
// The translation is the single source of truth for both the HOCON
// parser and the join-time configuration-compatibility check, so any
// silent regression here would cascade into config-incompatible joins.
func TestNormalizeDowningProviderClass(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"whitespace", "  \t  ", ""},
		{"pekko-fqcn", "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider", DefaultDowningProviderName},
		{"akka-fqcn", "akka.cluster.sbr.SplitBrainResolverProvider", DefaultDowningProviderName},
		{"short-name", DefaultDowningProviderName, DefaultDowningProviderName},
		{"unknown-fqcn", "com.example.MyDowningProvider", "com.example.MyDowningProvider"},
		{"unknown-short", "lease-driven", "lease-driven"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NormalizeDowningProviderClass(c.in); got != c.want {
				t.Errorf("NormalizeDowningProviderClass(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestDowningProviderRegistry_RoundTrip is the basic registration ↔
// lookup contract: registering a provider makes it resolvable by name,
// and Names returns the registered set.  Without this gekka.NewCluster
// would silently fall back to the default for every operator override.
func TestDowningProviderRegistry_RoundTrip(t *testing.T) {
	r := NewDowningProviderRegistry()
	p := &fakeDowningProvider{name: "lease-driven"}
	r.Register(p)

	got, ok := r.Resolve("lease-driven")
	if !ok {
		t.Fatal("Resolve returned ok=false for registered provider")
	}
	if got != p {
		t.Errorf("Resolve returned %v, want %v", got, p)
	}

	if _, ok := r.Resolve("not-registered"); ok {
		t.Error("Resolve returned ok=true for unregistered provider")
	}

	names := r.Names()
	if len(names) != 1 || names[0] != "lease-driven" {
		t.Errorf("Names = %v, want [lease-driven]", names)
	}
}

// TestDowningProviderRegistry_NilRegistration guards the registry
// against panicking on a nil provider.  Tests that bring up partial
// configurations rely on this being a silent no-op.
func TestDowningProviderRegistry_NilRegistration(t *testing.T) {
	r := NewDowningProviderRegistry()
	r.Register(nil)
	if names := r.Names(); len(names) != 0 {
		t.Errorf("nil Register should be no-op, got Names = %v", names)
	}
}

// TestDowningProviderRegistry_LastWriterWins documents the replace-on-
// re-register semantics.  Tests that swap a real provider for a fake
// inside a setup helper depend on this; if registry refused a second
// Register the test would silently exercise the original provider.
func TestDowningProviderRegistry_LastWriterWins(t *testing.T) {
	r := NewDowningProviderRegistry()
	first := &fakeDowningProvider{name: "x"}
	second := &fakeDowningProvider{name: "x"}
	r.Register(first)
	r.Register(second)
	got, _ := r.Resolve("x")
	if got != second {
		t.Errorf("Resolve = %v, want %v (last-writer-wins)", got, second)
	}
}

// TestSBRDowningProvider_Name pins the short name SBR registers under.
// The string is part of the operator-facing config surface; changing
// it would invisibly break every config that relied on the default
// fallback for downing-provider-class.
func TestSBRDowningProvider_Name(t *testing.T) {
	p := NewSBRDowningProvider(nil)
	if got, want := p.Name(), DefaultDowningProviderName; got != want {
		t.Errorf("SBRDowningProvider.Name = %q, want %q", got, want)
	}
}

// TestSBRDowningProvider_StartWithNilManager covers the SBR-disabled
// case: the cluster registers SBR even when active-strategy="" so the
// short name resolves cleanly, but Start must not panic on a nil
// manager.  This is exactly the path SBR-disabled clusters run.
func TestSBRDowningProvider_StartWithNilManager(t *testing.T) {
	p := NewSBRDowningProvider(nil)
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start with nil manager returned %v, want nil", err)
	}
}

// TestFormatUnknownProviderError exercises the error formatter that
// drives the operator-facing log line and the test fixtures below.
// Locking the format string here means the operator log won't drift
// silently when registry plumbing is touched in later sessions.
func TestFormatUnknownProviderError(t *testing.T) {
	available := []string{"a", "b"}
	sort.Strings(available)
	got := FormatUnknownProviderError("missing", available)
	if !strings.Contains(got, "missing") {
		t.Errorf("error %q should mention provider name", got)
	}
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") {
		t.Errorf("error %q should mention available providers %v", got, available)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 28 — SBR consolidated under provider
// ─────────────────────────────────────────────────────────────────────────────

// TestResolveSBRConfigDefaults_NonLeaseStrategiesUntouched is the
// regression guard for keep-majority / keep-oldest / static-quorum /
// keep-referee / down-all: the consolidation helper must not rewrite
// their LeaseSettings, since they don't use a lease at all.  A bug
// here would silently inject lease defaults into non-lease strategies
// and confuse the compatibility check at join time.
func TestResolveSBRConfigDefaults_NonLeaseStrategiesUntouched(t *testing.T) {
	for _, strategy := range []string{
		"keep-majority", "keep-oldest", "static-quorum",
		"keep-referee", "down-all",
	} {
		t.Run(strategy, func(t *testing.T) {
			in := SBRConfig{ActiveStrategy: strategy}
			d := SBRDefaults{
				LeaseProviderName: "memory",
				SystemName:        "TestSystem",
				Host:              "127.0.0.1",
				Port:              2552,
			}
			out := ResolveSBRConfigDefaults(in, d)
			if out.LeaseImplementation != "" {
				t.Errorf("non-lease strategy got LeaseImplementation = %q", out.LeaseImplementation)
			}
			if out.LeaseSettings.LeaseName != "" {
				t.Errorf("non-lease strategy got LeaseName = %q", out.LeaseSettings.LeaseName)
			}
			if out.LeaseSettings.OwnerName != "" {
				t.Errorf("non-lease strategy got OwnerName = %q", out.LeaseSettings.OwnerName)
			}
		})
	}
}

// TestResolveSBRConfigDefaults_LeaseMajorityFillsBlanks proves the
// helper applies the same defaults the inline cluster.go block did
// before S28 — empty LeaseImplementation, empty lease names, etc.
// A regression here would break HOCON-only deployments that rely on
// the in-memory reference provider as a default.
func TestResolveSBRConfigDefaults_LeaseMajorityFillsBlanks(t *testing.T) {
	d := SBRDefaults{
		LeaseProviderName: "memory",
		SystemName:        "TestSystem",
		Host:              "127.0.0.1",
		Port:              2552,
		LeaseDuration:     30 * time.Second,
		RetryInterval:     3 * time.Second,
	}
	out := ResolveSBRConfigDefaults(SBRConfig{ActiveStrategy: "lease-majority"}, d)
	if out.LeaseImplementation != "memory" {
		t.Errorf("LeaseImplementation = %q, want %q", out.LeaseImplementation, "memory")
	}
	if out.LeaseSettings.LeaseName != "TestSystem-pekko-sbr" {
		t.Errorf("LeaseName = %q, want TestSystem-pekko-sbr", out.LeaseSettings.LeaseName)
	}
	if out.LeaseSettings.OwnerName != "127.0.0.1:2552" {
		t.Errorf("OwnerName = %q, want 127.0.0.1:2552", out.LeaseSettings.OwnerName)
	}
	if out.LeaseSettings.LeaseDuration != 30*time.Second {
		t.Errorf("LeaseDuration = %v, want 30s", out.LeaseSettings.LeaseDuration)
	}
	if out.LeaseSettings.RetryInterval != 3*time.Second {
		t.Errorf("RetryInterval = %v, want 3s", out.LeaseSettings.RetryInterval)
	}
}

// TestResolveSBRConfigDefaults_LeaseMajorityFallsBackToPekkoDefaults
// pins the fallback constants (120s lease, 12s retry) so a default-only
// deployment stays at parity with Pekko reference.conf even when the
// CoordinationLease section is absent.
func TestResolveSBRConfigDefaults_LeaseMajorityFallsBackToPekkoDefaults(t *testing.T) {
	out := ResolveSBRConfigDefaults(SBRConfig{ActiveStrategy: "lease-majority"}, SBRDefaults{})
	if got, want := out.LeaseSettings.LeaseDuration, 120*time.Second; got != want {
		t.Errorf("LeaseDuration fallback = %v, want %v", got, want)
	}
	if got, want := out.LeaseSettings.RetryInterval, 12*time.Second; got != want {
		t.Errorf("RetryInterval fallback = %v, want %v", got, want)
	}
}

// TestResolveSBRConfigDefaults_PreservesOperatorOverrides confirms
// that operator-supplied values win over defaults — the helper only
// fills BLANK fields.  Without this, an explicit lease-implementation
// would be silently overwritten with the in-memory provider.
func TestResolveSBRConfigDefaults_PreservesOperatorOverrides(t *testing.T) {
	in := SBRConfig{
		ActiveStrategy:      "lease-majority",
		LeaseImplementation: "kubernetes-lease",
	}
	in.LeaseSettings.LeaseName = "custom-name"
	in.LeaseSettings.OwnerName = "custom-owner"
	in.LeaseSettings.LeaseDuration = 7 * time.Second
	in.LeaseSettings.RetryInterval = 1 * time.Second

	out := ResolveSBRConfigDefaults(in, SBRDefaults{
		LeaseProviderName: "memory",
		SystemName:        "OtherSystem",
		Host:              "0.0.0.0",
		Port:              99,
		LeaseDuration:     999 * time.Second,
		RetryInterval:     111 * time.Second,
	})
	if out.LeaseImplementation != "kubernetes-lease" {
		t.Errorf("LeaseImplementation overridden to %q", out.LeaseImplementation)
	}
	if out.LeaseSettings.LeaseName != "custom-name" {
		t.Errorf("LeaseName overridden to %q", out.LeaseSettings.LeaseName)
	}
	if out.LeaseSettings.OwnerName != "custom-owner" {
		t.Errorf("OwnerName overridden to %q", out.LeaseSettings.OwnerName)
	}
	if out.LeaseSettings.LeaseDuration != 7*time.Second {
		t.Errorf("LeaseDuration overridden to %v", out.LeaseSettings.LeaseDuration)
	}
	if out.LeaseSettings.RetryInterval != 1*time.Second {
		t.Errorf("RetryInterval overridden to %v", out.LeaseSettings.RetryInterval)
	}
}

// TestBuildSBRDowningProvider_AllStrategiesResolveByName is the S28
// acceptance test: every Pekko-supported SBR ActiveStrategy must
// reach a non-nil SBRDowningProvider through the public factory.
// `keep-referee` requires RefereeAddress to construct successfully.
// Anything else and the consolidation broke a strategy code path.
func TestBuildSBRDowningProvider_AllStrategiesResolveByName(t *testing.T) {
	cm := newTestClusterManager(t)


	cases := []struct {
		strategy string
		extra    func(c *SBRConfig)
	}{
		{"keep-majority", nil},
		{"keep-oldest", nil},
		{"static-quorum", func(c *SBRConfig) { c.QuorumSize = 1 }},
		{"keep-referee", func(c *SBRConfig) { c.RefereeAddress = "127.0.0.1:9999" }},
		{"lease-majority", nil},
		{"down-all", nil},
	}
	for _, tc := range cases {
		t.Run(tc.strategy, func(t *testing.T) {
			cfg := SBRConfig{ActiveStrategy: tc.strategy, StableAfter: time.Second}
			if tc.extra != nil {
				tc.extra(&cfg)
			}
			provider := BuildSBRDowningProvider(cm, cfg, SBRDefaults{
				LeaseProviderName: "memory",
				LeaseManager:      icluster.NewLeaseManager(),
				SystemName:        "TestSystem",
				Host:              "127.0.0.1",
				Port:              2552,
			})
			if provider == nil {
				t.Fatal("BuildSBRDowningProvider returned nil for a registered strategy")
			}
			if provider.Name() != DefaultDowningProviderName {
				t.Errorf("provider.Name = %q, want %q", provider.Name(), DefaultDowningProviderName)
			}
		})
	}
}

// TestBuildSBRDowningProvider_DisabledStrategyStillRegisters covers the
// SBR-disabled cluster: empty ActiveStrategy must still produce a
// non-nil provider so the cluster's DowningProviderRegistry resolves
// the canonical short name without falling through to "unknown
// provider" plus a fallback log line.
func TestBuildSBRDowningProvider_DisabledStrategyStillRegisters(t *testing.T) {
	cm := newTestClusterManager(t)


	provider := BuildSBRDowningProvider(cm, SBRConfig{}, SBRDefaults{})
	if provider == nil {
		t.Fatal("BuildSBRDowningProvider returned nil for SBR-disabled config")
	}
	if provider.Manager() != nil {
		t.Errorf("expected Manager() to be nil for empty SBRConfig, got %+v", provider.Manager())
	}
}

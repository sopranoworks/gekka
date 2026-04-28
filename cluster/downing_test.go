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
)

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

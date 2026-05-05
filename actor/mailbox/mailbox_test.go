/*
 * mailbox_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"testing"
)

// ── Registry ────────────────────────────────────────────────────────────────

type stubFactory struct{ tag string }

func (s stubFactory) NewMailbox(_ Config) Mailbox { return nil }

func TestRegistry_RegisterAndLookup(t *testing.T) {
	f := stubFactory{tag: "alpha"}
	Register(f, "test/alpha", "test/alpha-alias")
	t.Cleanup(func() {
		registryMu.Lock()
		delete(registry, "test/alpha")
		delete(registry, "test/alpha-alias")
		registryMu.Unlock()
	})

	if got := Lookup("test/alpha"); got != f {
		t.Fatalf("Lookup(test/alpha) = %v, want %v", got, f)
	}
	if got := Lookup("test/alpha-alias"); got != f {
		t.Fatalf("Lookup(test/alpha-alias) = %v, want %v", got, f)
	}
}

func TestRegistry_RegisterReplaces(t *testing.T) {
	a := stubFactory{tag: "first"}
	b := stubFactory{tag: "second"}
	Register(a, "test/replaces")
	Register(b, "test/replaces")
	t.Cleanup(func() {
		registryMu.Lock()
		delete(registry, "test/replaces")
		registryMu.Unlock()
	})
	if got := Lookup("test/replaces"); got != b {
		t.Fatalf("Lookup after replace = %v, want %v", got, b)
	}
}

func TestRegistry_LookupUnknownReturnsNil(t *testing.T) {
	if got := Lookup("test/never-registered"); got != nil {
		t.Fatalf("Lookup(unknown) = %v, want nil", got)
	}
}

func TestRegistry_DefaultIsUnboundedFactory(t *testing.T) {
	def := Default()
	if def == nil {
		t.Fatal("Default() returned nil")
	}
	mb := def.NewMailbox(Config{})
	if _, ok := mb.(*unboundedMailbox); !ok {
		t.Fatalf("Default factory produced %T, want *unboundedMailbox", mb)
	}
	mb.Close()
}

func TestRegistry_ResolveEmptyReturnsDefault(t *testing.T) {
	got := Resolve("")
	if got != Default() {
		t.Fatalf("Resolve(\"\") = %v, want default", got)
	}
}

func TestRegistry_ResolveKnown(t *testing.T) {
	cases := []string{
		"unbounded",
		"org.apache.pekko.dispatch.UnboundedMailbox",
		"akka.dispatch.UnboundedMailbox",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			f := Resolve(id)
			if f == nil {
				t.Fatalf("Resolve(%q) = nil, want unbounded factory", id)
			}
			mb := f.NewMailbox(Config{})
			if _, ok := mb.(*unboundedMailbox); !ok {
				t.Fatalf("Resolve(%q) factory produced %T, want *unboundedMailbox", id, mb)
			}
			mb.Close()
		})
	}
}

func TestRegistry_ResolveUnknownReturnsNil(t *testing.T) {
	if got := Resolve("test/unknown-mailbox-type"); got != nil {
		t.Fatalf("Resolve(unknown) = %v, want nil (fatal-config policy)", got)
	}
}

/*
 * replicator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"testing"
)

func TestReplicator_Entries_Empty(t *testing.T) {
	r := NewReplicator("node-1", nil)
	if got := r.Entries(); len(got) != 0 {
		t.Errorf("want empty, got %v", got)
	}
}

func TestReplicator_Entries_OneOfEach(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.GCounter("counter-1")
	r.ORSet("set-1")
	r.LWWMap("map-1")
	r.PNCounter("pn-1")
	r.ORFlag("flag-1")
	r.LWWRegister("reg-1")

	got := r.Entries()
	if len(got) != 6 {
		t.Fatalf("want 6 entries, got %d: %v", len(got), got)
	}
	byType := map[string]string{}
	for _, e := range got {
		byType[e.Type] = e.Key
	}
	want := map[string]string{
		"gcounter":    "counter-1",
		"orset":       "set-1",
		"lwwmap":      "map-1",
		"pncounter":   "pn-1",
		"orflag":      "flag-1",
		"lwwregister": "reg-1",
	}
	for typ, expectedKey := range want {
		if byType[typ] != expectedKey {
			t.Errorf("type %q: want key %q, got %q", typ, expectedKey, byType[typ])
		}
	}
}

func TestReplicator_LookupGCounter_HitAndMiss(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.GCounter("existing")

	got, ok := r.LookupGCounter("existing")
	if !ok || got == nil {
		t.Errorf("hit: want (non-nil, true), got (%v, %v)", got, ok)
	}
	got, ok = r.LookupGCounter("missing")
	if ok || got != nil {
		t.Errorf("miss: want (nil, false), got (%v, %v)", got, ok)
	}
	// Crucial: Lookup must NOT have created "missing".
	if _, exists := r.counters["missing"]; exists {
		t.Error("Lookup on miss must not create the entry")
	}
}

func TestReplicator_LookupORSet_HitAndMiss(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.ORSet("existing")
	got, ok := r.LookupORSet("existing")
	if !ok || got == nil {
		t.Errorf("hit: want (non-nil, true), got (%v, %v)", got, ok)
	}
	_, ok = r.LookupORSet("missing")
	if ok {
		t.Error("miss: expected ok=false")
	}
}

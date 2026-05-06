/*
 * requirements_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"strings"
	"testing"
	"time"
)

// ── Test fixtures ───────────────────────────────────────────────────────────

type ctrlAwareActor struct{}

func (ctrlAwareActor) RequiresControlAwareMessageQueueSemantics() {}

type plainActor struct{}

// ── requirementOf ───────────────────────────────────────────────────────────

func TestRequirementOf_DetectsControlAwareMarker(t *testing.T) {
	if got := requirementOf(ctrlAwareActor{}); got != RequirementControlAwareMessageQueueSemanticsID {
		t.Fatalf("requirementOf(ctrlAwareActor) = %q, want %q",
			got, RequirementControlAwareMessageQueueSemanticsID)
	}
}

func TestRequirementOf_PlainActorReturnsEmpty(t *testing.T) {
	if got := requirementOf(plainActor{}); got != "" {
		t.Fatalf("requirementOf(plainActor) = %q, want empty", got)
	}
}

func TestRequirementOf_NilReturnsEmpty(t *testing.T) {
	if got := requirementOf(nil); got != "" {
		t.Fatalf("requirementOf(nil) = %q, want empty", got)
	}
}

// ── Capability registry ─────────────────────────────────────────────────────

func TestFactoryCapabilities_BuiltInControlAware(t *testing.T) {
	caps := FactoryCapabilities("unbounded-control-aware")
	if len(caps) == 0 {
		t.Fatalf("FactoryCapabilities(unbounded-control-aware) = empty; expected ControlAware caps")
	}
	want := RequirementControlAwareMessageQueueSemanticsID
	found := false
	for _, c := range caps {
		if c == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("FactoryCapabilities(unbounded-control-aware) missing %q; got %v", want, caps)
	}
}

func TestFactoryCapabilities_BuiltInBoundedControlAware(t *testing.T) {
	caps := FactoryCapabilities("bounded-control-aware")
	want := RequirementBoundedControlAwareID
	found := false
	for _, c := range caps {
		if c == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("FactoryCapabilities(bounded-control-aware) missing %q; got %v", want, caps)
	}
}

func TestFactoryCapabilities_UnknownReturnsNil(t *testing.T) {
	if caps := FactoryCapabilities("nonexistent-factory"); caps != nil {
		t.Fatalf("FactoryCapabilities(unknown) = %v, want nil", caps)
	}
}

// ── ResolveForActor ─────────────────────────────────────────────────────────

func TestResolveForActor_ExplicitWins(t *testing.T) {
	id, driven := ResolveForActor(ctrlAwareActor{}, "bounded", nil)
	if id != "bounded" {
		t.Fatalf("ResolveForActor explicit = %q, want bounded", id)
	}
	if driven {
		t.Fatalf("ResolveForActor explicit reported requirementDriven=true; expected false")
	}
}

func TestResolveForActor_RequirementDrivenDefault(t *testing.T) {
	id, driven := ResolveForActor(ctrlAwareActor{}, "", nil)
	if id != "unbounded-control-aware" {
		t.Fatalf("ResolveForActor requirement-default = %q, want unbounded-control-aware", id)
	}
	if !driven {
		t.Fatalf("ResolveForActor requirement-driven reported requirementDriven=false; expected true")
	}
}

func TestResolveForActor_BindingsOverrideDefault(t *testing.T) {
	b := NewBindings()
	b.Set(RequirementControlAwareMessageQueueSemanticsID, "bounded-control-aware")
	id, driven := ResolveForActor(ctrlAwareActor{}, "", b)
	if id != "bounded-control-aware" {
		t.Fatalf("ResolveForActor with binding = %q, want bounded-control-aware", id)
	}
	if !driven {
		t.Fatalf("expected requirementDriven=true after binding match")
	}
}

func TestResolveForActor_PlainActorFallsBackToDefault(t *testing.T) {
	id, driven := ResolveForActor(plainActor{}, "", nil)
	if id != DefaultMailboxID {
		t.Fatalf("ResolveForActor plain = %q, want %q", id, DefaultMailboxID)
	}
	if driven {
		t.Fatalf("plain actor should not report requirementDriven")
	}
}

// ── ValidateRequirement ─────────────────────────────────────────────────────

func TestValidateRequirement_PlainActorOK(t *testing.T) {
	if err := ValidateRequirement(plainActor{}, "unbounded"); err != nil {
		t.Fatalf("plain actor on unbounded should validate; got %v", err)
	}
	if err := ValidateRequirement(plainActor{}, "bounded-control-aware"); err != nil {
		t.Fatalf("plain actor accepts any factory; got %v", err)
	}
}

func TestValidateRequirement_ControlAwareOnControlAwareFactoryOK(t *testing.T) {
	for _, fid := range []string{
		"unbounded-control-aware",
		"bounded-control-aware",
		"org.apache.pekko.dispatch.UnboundedControlAwareMailbox",
		"akka.dispatch.UnboundedControlAwareMailbox",
		"org.apache.pekko.dispatch.BoundedControlAwareMailbox",
		"akka.dispatch.BoundedControlAwareMailbox",
	} {
		if err := ValidateRequirement(ctrlAwareActor{}, fid); err != nil {
			t.Fatalf("ValidateRequirement(ctrlAware, %q) = %v, want nil", fid, err)
		}
	}
}

func TestValidateRequirement_ControlAwareOnNonControlAwareFails(t *testing.T) {
	err := ValidateRequirement(ctrlAwareActor{}, "unbounded")
	if err == nil {
		t.Fatalf("ValidateRequirement(ctrlAware, unbounded) = nil, want hard error")
	}
	// The error message must reference both the requirement and the
	// resolved factory so users can diagnose the mismatch.
	msg := err.Error()
	if !strings.Contains(msg, RequirementControlAwareMessageQueueSemanticsID) {
		t.Errorf("error %q missing requirement id", msg)
	}
	if !strings.Contains(msg, "unbounded") {
		t.Errorf("error %q missing factory id", msg)
	}
}

func TestValidateRequirement_ControlAwareOnUnknownFactoryFails(t *testing.T) {
	if err := ValidateRequirement(ctrlAwareActor{}, "nonexistent-factory"); err == nil {
		t.Fatalf("expected hard error when resolved factory has no capabilities")
	}
}

// ── DefaultFactoryFor ───────────────────────────────────────────────────────

func TestDefaultFactoryFor_KnownRequirements(t *testing.T) {
	cases := map[string]string{
		RequirementControlAwareMessageQueueSemanticsID:     "unbounded-control-aware",
		RequirementUnboundedControlAwareID:                 "unbounded-control-aware",
		RequirementBoundedControlAwareID:                   "bounded-control-aware",
		AkkaRequirementControlAwareMessageQueueSemanticsID: "unbounded-control-aware",
		AkkaRequirementBoundedControlAwareID:               "bounded-control-aware",
	}
	for req, want := range cases {
		if got := DefaultFactoryFor(req); got != want {
			t.Errorf("DefaultFactoryFor(%q) = %q, want %q", req, got, want)
		}
	}
}

func TestDefaultFactoryFor_UnknownReturnsEmpty(t *testing.T) {
	if got := DefaultFactoryFor("not.a.known.requirement"); got != "" {
		t.Fatalf("DefaultFactoryFor(unknown) = %q, want empty", got)
	}
}

// ── Bindings ────────────────────────────────────────────────────────────────

func TestBindings_SetGet(t *testing.T) {
	b := NewBindings()
	b.Set("req-1", "factory-1")
	b.Set("req-2", "factory-2")
	if got := b.Get("req-1"); got != "factory-1" {
		t.Errorf("Get req-1 = %q, want factory-1", got)
	}
	if got := b.Get("req-2"); got != "factory-2" {
		t.Errorf("Get req-2 = %q, want factory-2", got)
	}
	if b.Len() != 2 {
		t.Errorf("Len = %d, want 2", b.Len())
	}
}

func TestBindings_LastWriteWins(t *testing.T) {
	b := NewBindings()
	b.Set("req", "factory-A")
	b.Set("req", "factory-B")
	if got := b.Get("req"); got != "factory-B" {
		t.Errorf("Get after overwrite = %q, want factory-B", got)
	}
}

func TestBindings_ZeroValueSafe(t *testing.T) {
	var b Bindings
	if got := b.Get("anything"); got != "" {
		t.Errorf("zero-value Get = %q, want empty", got)
	}
	b.Set("req", "factory")
	if got := b.Get("req"); got != "factory" {
		t.Errorf("zero-value Set then Get = %q, want factory", got)
	}
}

// ── Global config ───────────────────────────────────────────────────────────

func TestSetGlobalConfig_RoundTrip(t *testing.T) {
	// Save and restore so this test does not pollute siblings.
	prevType := GlobalDefaultType()
	prevDefaults := GlobalDefaults()
	prevBindings := GlobalBindings()
	t.Cleanup(func() {
		// Round-trip the previous bindings into a fresh map.
		bm := map[string]string{}
		if prevBindings != nil {
			prevBindings.mu.RLock()
			for k, v := range prevBindings.tbl {
				bm[k] = v
			}
			prevBindings.mu.RUnlock()
		}
		SetGlobalConfig(prevType, prevDefaults.Capacity, prevDefaults.PushTimeout, bm)
	})

	SetGlobalConfig(
		"bounded",
		512,
		750*time.Millisecond,
		map[string]string{
			RequirementControlAwareMessageQueueSemanticsID: "bounded-control-aware",
		},
	)

	if got := GlobalDefaultType(); got != "bounded" {
		t.Errorf("GlobalDefaultType = %q, want bounded", got)
	}
	if got := GlobalDefaults(); got.Capacity != 512 || got.PushTimeout != 750*time.Millisecond {
		t.Errorf("GlobalDefaults = %+v, want {512, 750ms}", got)
	}
	if got := GlobalBindings().Get(RequirementControlAwareMessageQueueSemanticsID); got != "bounded-control-aware" {
		t.Errorf("GlobalBindings().Get(ctrlAware) = %q, want bounded-control-aware", got)
	}
}

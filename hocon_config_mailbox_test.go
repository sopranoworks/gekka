/*
 * hocon_config_mailbox_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"
)

// ── pekko.actor.default-mailbox.* ───────────────────────────────────────────

func TestHOCON_DefaultMailbox_AllKeys(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.actor.default-mailbox {
  mailbox-type             = "bounded"
  mailbox-capacity         = 2048
  mailbox-push-timeout-time = "250ms"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Mailbox.DefaultType, "bounded"; got != want {
		t.Errorf("DefaultType = %q, want %q", got, want)
	}
	if got, want := cfg.Mailbox.DefaultCapacity, 2048; got != want {
		t.Errorf("DefaultCapacity = %d, want %d", got, want)
	}
	if got, want := cfg.Mailbox.DefaultPushTimeout, 250*time.Millisecond; got != want {
		t.Errorf("DefaultPushTimeout = %v, want %v", got, want)
	}
}

func TestHOCON_DefaultMailbox_FQCNAccepted(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.actor.default-mailbox.mailbox-type = "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Mailbox.DefaultType, "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"; got != want {
		t.Errorf("DefaultType = %q, want FQCN %q", got, want)
	}
}

func TestHOCON_DefaultMailbox_UnsetFallsBackToEmpty(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// SpawnActor's resolvePhase1Mailbox treats empty DefaultType as
	// "fall through to mailbox.DefaultMailboxID = unbounded". The parser
	// itself must report empty so the consumer can apply that rule.
	if cfg.Mailbox.DefaultType != "" {
		t.Errorf("DefaultType when unset = %q, want empty (caller falls back to unbounded)",
			cfg.Mailbox.DefaultType)
	}
}

// ── pekko.actor.mailbox.requirements.* ──────────────────────────────────────

func TestHOCON_MailboxRequirements_DirectFactoryID(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.actor.mailbox.requirements {
  "org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"        = "unbounded-control-aware"
  "org.apache.pekko.dispatch.BoundedControlAwareMessageQueueSemantics" = "bounded-control-aware"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.Mailbox.Requirements
	if got["org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"] != "unbounded-control-aware" {
		t.Errorf("ControlAware binding = %q, want unbounded-control-aware",
			got["org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"])
	}
	if got["org.apache.pekko.dispatch.BoundedControlAwareMessageQueueSemantics"] != "bounded-control-aware" {
		t.Errorf("BoundedControlAware binding = %q, want bounded-control-aware",
			got["org.apache.pekko.dispatch.BoundedControlAwareMessageQueueSemantics"])
	}
}

func TestHOCON_MailboxRequirements_SubBlockResolvesToMailboxType(t *testing.T) {
	// Pekko's reference.conf maps requirement → block-path; the binding map
	// must dereference the path's mailbox-type, not store the path itself.
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.actor.mailbox.requirements {
  "org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics" = "pekko.dispatch.UnboundedControlAwareMailbox"
}
pekko.dispatch.UnboundedControlAwareMailbox {
  mailbox-type = "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.Mailbox.Requirements["org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"]
	if got != "org.apache.pekko.dispatch.UnboundedControlAwareMailbox" {
		t.Errorf("requirement → mailbox-type indirection = %q, want resolved FQCN", got)
	}
}

// ── pekko.dispatch.{Unbounded,Bounded}ControlAwareMailbox seed ──────────────

func TestHOCON_DispatchSeed_UnboundedControlAware(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.dispatch.UnboundedControlAwareMailbox {
  mailbox-type = "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.Mailbox.Requirements
	want := "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
	for _, req := range []string{
		"org.apache.pekko.dispatch.UnboundedControlAwareMessageQueueSemantics",
		"org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics",
		"akka.dispatch.UnboundedControlAwareMessageQueueSemantics",
		"akka.dispatch.ControlAwareMessageQueueSemantics",
	} {
		if got[req] != want {
			t.Errorf("seed requirement %q = %q, want %q", req, got[req], want)
		}
	}
}

func TestHOCON_DispatchSeed_BoundedControlAware(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.dispatch.BoundedControlAwareMailbox {
  mailbox-type = "org.apache.pekko.dispatch.BoundedControlAwareMailbox"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.Mailbox.Requirements
	want := "org.apache.pekko.dispatch.BoundedControlAwareMailbox"
	for _, req := range []string{
		"org.apache.pekko.dispatch.BoundedControlAwareMessageQueueSemantics",
		"akka.dispatch.BoundedControlAwareMessageQueueSemantics",
	} {
		if got[req] != want {
			t.Errorf("seed requirement %q = %q, want %q", req, got[req], want)
		}
	}
}

func TestHOCON_DispatchSeed_ExplicitRequirementOverridesSeed(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.actor.mailbox.requirements {
  "org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics" = "bounded-control-aware"
}
pekko.dispatch.UnboundedControlAwareMailbox {
  mailbox-type = "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.Mailbox.Requirements["org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"]
	if got != "bounded-control-aware" {
		t.Errorf("explicit requirement should win over seed; got %q, want bounded-control-aware", got)
	}
}

// ── passivate-idle-entity-after deprecated alias ────────────────────────────

func TestHOCON_PassivateIdleEntityAfter_AliasResolvesToCanonicalTimeout(t *testing.T) {
	// When only the deprecated alias is set, it must populate
	// PassivationIdleTimeout (the canonical consumer field).
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivate-idle-entity-after = "3m"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 3*time.Minute {
		t.Errorf("PassivationIdleTimeout via alias = %v, want 3m",
			cfg.Sharding.PassivationIdleTimeout)
	}
}

func TestHOCON_PassivateIdleEntityAfter_CanonicalWinsOverAlias(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = "7m"
pekko.cluster.sharding.passivate-idle-entity-after = "3m"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 7*time.Minute {
		t.Errorf("canonical should win; got %v, want 7m", cfg.Sharding.PassivationIdleTimeout)
	}
}

func TestHOCON_PassivateIdleEntityAfter_AliasOffDisablesPassivation(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivate-idle-entity-after = "off"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 0 {
		t.Errorf("alias=off should disable passivation; got %v",
			cfg.Sharding.PassivationIdleTimeout)
	}
}

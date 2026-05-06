/*
 * requirements.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package mailbox

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// MailboxRequirement is the marker every requirement type satisfies. Concrete
// requirement interfaces (RequiresControlAwareMessageQueueSemantics and friends)
// embed it indirectly by declaring their canonical ID via the Requirement
// method. The mailbox subsystem keys its bindings table off the requirement ID
// rather than the Go type so the actor package can use type aliases for
// re-exports without breaking the binding map.
//
// User actors satisfy a requirement by implementing one of the concrete
// requirement interfaces below. The system inspects the actor instance via a
// type assertion at construction time; a mismatch between a declared
// requirement and the configured mailbox factory is a hard error.
type MailboxRequirement interface {
	// MailboxRequirementID returns the canonical string identifier used by the
	// HOCON binding map. The identifier must be stable across releases — it is
	// part of the configuration surface.
	MailboxRequirementID() string
}

// ── Requirement IDs ─────────────────────────────────────────────────────────
//
// These match Pekko's RequiresMessageQueue type-class constraints. The IDs use
// the Pekko/Akka FQCNs as the canonical form so HOCON
// `pekko.actor.mailbox.requirements.<FQCN>` keys round-trip without
// translation. Short aliases let test code and gekka-native callers spell the
// same requirement without typing the full FQCN.

// RequirementControlAwareMessageQueueSemanticsID is the canonical requirement
// ID for the ControlAware family. Pekko exposes three variants
// (ControlAware, UnboundedControlAware, BoundedControlAware); gekka collapses
// them into a single capability check because both gekka factories
// (unbounded and bounded) satisfy the broader ControlAware contract.
const RequirementControlAwareMessageQueueSemanticsID = "org.apache.pekko.dispatch.ControlAwareMessageQueueSemantics"

// RequirementUnboundedControlAwareID and RequirementBoundedControlAwareID are
// the variant FQCNs that map to the same gekka capability. They exist so the
// HOCON binding map can use either spelling.
const (
	RequirementUnboundedControlAwareID = "org.apache.pekko.dispatch.UnboundedControlAwareMessageQueueSemantics"
	RequirementBoundedControlAwareID   = "org.apache.pekko.dispatch.BoundedControlAwareMessageQueueSemantics"
)

// AkkaRequirementControlAwareMessageQueueSemanticsID and friends are the
// Akka-spelling aliases. Same semantic content as the Pekko spellings; both
// resolve to the same factory through the bindings table.
const (
	AkkaRequirementControlAwareMessageQueueSemanticsID = "akka.dispatch.ControlAwareMessageQueueSemantics"
	AkkaRequirementUnboundedControlAwareID             = "akka.dispatch.UnboundedControlAwareMessageQueueSemantics"
	AkkaRequirementBoundedControlAwareID               = "akka.dispatch.BoundedControlAwareMessageQueueSemantics"
)

// ── Marker interfaces ───────────────────────────────────────────────────────

// RequiresControlAwareMessageQueueSemantics is implemented by actors that
// rely on system-message priority (ControlMessage instances dispatched ahead
// of regular user messages). Pekko spells this requirement as
// RequiresMessageQueue[ControlAwareMessageQueueSemantics]; gekka uses a
// public marker method so user actors can satisfy it via:
//
//	type MyActor struct{ actor.BaseActor }
//	func (*MyActor) Receive(msg any) {}
//	func (*MyActor) RequiresControlAwareMessageQueueSemantics() {}
//
// At actor construction time the system asserts the configured mailbox
// factory satisfies the ControlAware capability. Mismatch is a hard error.
type RequiresControlAwareMessageQueueSemantics interface {
	RequiresControlAwareMessageQueueSemantics()
}

// requirementOf inspects an actor instance and returns the canonical
// requirement ID it declares, or "" if it declares none. Multiple
// requirements per actor are not supported — Pekko itself disallows it.
func requirementOf(a any) string {
	if a == nil {
		return ""
	}
	if _, ok := a.(RequiresControlAwareMessageQueueSemantics); ok {
		return RequirementControlAwareMessageQueueSemanticsID
	}
	return ""
}

// ── Capability registry ─────────────────────────────────────────────────────
//
// Each built-in factory advertises which requirement IDs it satisfies. The
// validation step at construction time looks up the configured factory's
// capability set and checks the actor's declared requirement against it.
//
// We use a side-table rather than embedding the capability into the factory
// interface because:
//   - The MailboxFactory interface was locked at sub-commit 1.1 with a
//     single method (NewMailbox); sub-commit 1.6 must not break that
//     contract for downstream factories.
//   - Capability is a property of the factory's IDs, not the factory
//     instance — every Pekko-spelling alias maps to the same capability.

var (
	capabilityMu  sync.RWMutex
	factoryCapMap = map[string][]string{}
)

// RegisterCapabilities associates one or more requirement IDs with each of
// the supplied factory IDs. Called from init() in mailbox files for the
// built-in factories.
func RegisterCapabilities(requirementIDs []string, factoryIDs ...string) {
	capabilityMu.Lock()
	defer capabilityMu.Unlock()
	for _, fid := range factoryIDs {
		factoryCapMap[fid] = append(factoryCapMap[fid], requirementIDs...)
	}
}

// FactoryCapabilities returns the requirement IDs satisfied by the factory
// registered under factoryID. An unknown factoryID returns nil (no
// capabilities), which causes ValidateRequirement to reject any actor with a
// declared requirement.
func FactoryCapabilities(factoryID string) []string {
	capabilityMu.RLock()
	defer capabilityMu.RUnlock()
	caps := factoryCapMap[factoryID]
	if caps == nil {
		return nil
	}
	out := make([]string, len(caps))
	copy(out, caps)
	return out
}

// satisfies reports whether factoryID's capability set includes requirementID.
func satisfies(factoryID, requirementID string) bool {
	for _, c := range FactoryCapabilities(factoryID) {
		if c == requirementID {
			return true
		}
	}
	return false
}

// ── Bindings (HOCON requirements map) ───────────────────────────────────────
//
// pekko.actor.mailbox.requirements.<requirement-FQCN> = <mailbox-type-id>
//
// The map is keyed by requirement ID and resolves to a factory ID. When an
// actor declares a requirement and HOCON has a binding for it, the binding
// supplies the mailbox factory. When the requirement has no HOCON binding
// the system falls back to a built-in default (e.g. control-aware →
// "unbounded-control-aware").

// Bindings is the in-memory representation of the requirements map. It is
// populated from HOCON at config-load time and consulted at SpawnActor time.
// A zero-value Bindings is safe — it just always falls back to defaults.
type Bindings struct {
	mu  sync.RWMutex
	tbl map[string]string
}

// NewBindings returns an empty Bindings ready for population.
func NewBindings() *Bindings { return &Bindings{tbl: map[string]string{}} }

// Set associates requirementID with factoryID. Last write wins, matching
// Pekko's HOCON merge semantics.
func (b *Bindings) Set(requirementID, factoryID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tbl == nil {
		b.tbl = map[string]string{}
	}
	b.tbl[requirementID] = factoryID
}

// Get returns the factory ID bound to requirementID, or "" if no binding
// exists.
func (b *Bindings) Get(requirementID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tbl[requirementID]
}

// Len returns the number of bindings. Useful for tests asserting that the
// HOCON parse populated the map.
func (b *Bindings) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.tbl)
}

// DefaultFactoryFor returns the built-in factory ID for requirementID, or ""
// if the requirement has no built-in default. Used as a fallback when HOCON
// supplies no binding.
func DefaultFactoryFor(requirementID string) string {
	switch requirementID {
	case RequirementControlAwareMessageQueueSemanticsID,
		RequirementUnboundedControlAwareID,
		AkkaRequirementControlAwareMessageQueueSemanticsID,
		AkkaRequirementUnboundedControlAwareID:
		return "unbounded-control-aware"
	case RequirementBoundedControlAwareID,
		AkkaRequirementBoundedControlAwareID:
		return "bounded-control-aware"
	}
	return ""
}

// ── Resolution + validation ─────────────────────────────────────────────────

// ResolveForActor returns the factory ID that should be used for actor a,
// given the supplied explicit factoryID (from Props.WithMailbox or HOCON
// default-mailbox.mailbox-type) and the bindings map.
//
// Precedence:
//  1. Explicit factoryID wins if non-empty.
//  2. If the actor declares a requirement and HOCON has a binding for it,
//     the binding wins.
//  3. If the actor declares a requirement and no binding exists, the
//     built-in default for that requirement wins.
//  4. Otherwise the caller's default factory (DefaultMailboxID) is used.
//
// The returned bool indicates whether the resolved factory was driven by a
// requirement (true) or by the explicit/default path (false). Callers use it
// to decide whether ValidateRequirement should run.
func ResolveForActor(a any, explicitFactoryID string, bindings *Bindings) (factoryID string, requirementDriven bool) {
	if explicitFactoryID != "" {
		return explicitFactoryID, false
	}
	req := requirementOf(a)
	if req == "" {
		return DefaultMailboxID, false
	}
	if bindings != nil {
		if id := bindings.Get(req); id != "" {
			return id, true
		}
	}
	if id := DefaultFactoryFor(req); id != "" {
		return id, true
	}
	return DefaultMailboxID, true
}

// ValidateRequirement checks whether actor a's declared requirement is
// satisfied by factoryID. Returns nil when the actor declares no requirement,
// or when the factory satisfies the requirement. Returns an error when the
// actor declares a requirement and the factory does not satisfy it — the
// caller (SpawnActor) must propagate this as a hard error to the user, not
// silently fall back to a working default.
//
// This is the construction-time hard-error guarantee mandated by Phase 1.6:
// a bound mismatch fails actor start.
func ValidateRequirement(a any, factoryID string) error {
	req := requirementOf(a)
	if req == "" {
		return nil
	}
	if satisfies(factoryID, req) {
		return nil
	}
	return fmt.Errorf(
		"mailbox: actor declares requirement %q but configured mailbox %q does not satisfy it",
		req, factoryID,
	)
}

// ── Global bindings + defaults (HOCON-driven) ───────────────────────────────
//
// HOCON parsing happens once per process (in hocon_config.go's
// hoconToClusterConfig) and the resulting bindings + default mailbox config
// must reach SpawnActor without threading three layers of struct plumbing.
// We expose package-level setters that the HOCON parser calls and matching
// readers that the actor cell consumes — the same pattern actor's
// RegisterDispatcherConfig uses for dispatcher metadata.

var (
	globalMu       sync.RWMutex
	globalBindings = NewBindings()
	globalDefaults Config
	globalDefType  string
)

// SetGlobalConfig installs the parsed pekko.actor.default-mailbox.* and
// pekko.actor.mailbox.requirements.* values for use by SpawnActor.
//
// Calling SetGlobalConfig with a nil bindings argument resets the bindings
// to an empty map but keeps the defaults. Repeated calls are last-write-
// wins — Pekko HOCON merge semantics — so layered configs (reference.conf
// + application.conf) compose correctly.
func SetGlobalConfig(defaultType string, defaultCapacity int, defaultPushTimeout time.Duration, bindings map[string]string) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalDefType = strings.TrimSpace(defaultType)
	globalDefaults = Config{Capacity: defaultCapacity, PushTimeout: defaultPushTimeout}
	b := NewBindings()
	for k, v := range bindings {
		b.Set(k, v)
	}
	globalBindings = b
}

// GlobalBindings returns the currently-installed requirements binding map.
// SpawnActor consults it after Props.MailboxName but before falling back to
// the system default.
func GlobalBindings() *Bindings {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalBindings
}

// GlobalDefaults returns the parsed pekko.actor.default-mailbox capacity +
// push-timeout. Used by SpawnActor when constructing a Mailbox via
// MailboxFactory.NewMailbox(cfg).
func GlobalDefaults() Config {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalDefaults
}

// GlobalDefaultType returns the parsed
// pekko.actor.default-mailbox.mailbox-type, or "" if HOCON did not set it.
func GlobalDefaultType() string {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalDefType
}

/*
 * telemetry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import "github.com/sopranoworks/gekka/telemetry"

// ── Package-level telemetry provider ─────────────────────────────────────────

// actorTelemetry is the telemetry provider used by actor instrumentation.
// It is always read from the global telemetry provider, so registering a
// provider with telemetry.SetProvider is sufficient to enable instrumentation.

func actorTracer() telemetry.Tracer {
	return telemetry.Global().Tracer("github.com/sopranoworks/gekka/actor")
}

func actorMeter() telemetry.Meter {
	return telemetry.Global().Meter("github.com/sopranoworks/gekka/actor")
}

// initActorMetrics ensures the two shared instruments are created.
// Called from Start; safe to call multiple times (instruments are idempotent).
func initActorMetrics() (telemetry.UpDownCounter, telemetry.Histogram) {
	m := actorMeter()
	mbs := m.UpDownCounter(
		"gekka_actor_mailbox_size",
		"Current number of messages waiting in the actor mailbox.",
		"{messages}",
	)
	pd := m.Histogram(
		"gekka_actor_message_process_duration",
		"Time spent executing the actor Receive method.",
		"s",
	)
	return mbs, pd
}

/*
 * doc.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package otel provides an OpenTelemetry-backed implementation of the
// gekka telemetry.Provider interface.
//
// This extension module is intentionally separate from the core gekka module
// so that applications that do not use OpenTelemetry incur zero dependency
// cost from the OTel SDK.
//
// The source code currently lives in the core repository at
// telemetry/otel/ and will be migrated here in v0.13.2-dev.
//
// Usage (future):
//
//	import (
//	    "github.com/sopranoworks/gekka/telemetry"
//	    gekkaotel "github.com/sopranoworks/gekka-extensions-telemetry-otel"
//	)
//
//	telemetry.SetProvider(gekkaotel.NewProvider())
package otel

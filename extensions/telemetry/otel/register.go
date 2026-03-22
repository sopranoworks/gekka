/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package otel

import "github.com/sopranoworks/gekka/telemetry"

func init() {
	telemetry.RegisterProvider("otel", func() telemetry.Provider {
		return NewProvider()
	})
}

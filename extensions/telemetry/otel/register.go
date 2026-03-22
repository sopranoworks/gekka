/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package otel

import (
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/telemetry"
)

func init() {
	telemetry.RegisterProvider("otel", func(_ hocon.Config) (telemetry.Provider, error) {
		return NewProvider(), nil
	})
}

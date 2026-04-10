/*
 * deployment_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"testing"

	hocon "github.com/sopranoworks/gekka-config"
)

func TestDeploymentConfig_Resizer(t *testing.T) {
	input := `
pekko.actor.deployment {
  "/user/myPool" {
    router = round-robin-pool
    nr-of-instances = 3
    resizer {
      lower-bound = 2
      upper-bound = 15
      pressure-threshold = 3
      rampup-rate = 0.25
      backoff-rate = 0.1
      backoff-threshold = 0.4
      messages-per-resize = 20
    }
  }
}
`
	cfg, err := hocon.ParseString(input)
	if err != nil {
		t.Fatalf("failed to parse HOCON: %v", err)
	}

	dc, ok := LookupDeployment(cfg, "/user/myPool")
	if !ok {
		t.Fatal("expected deployment config to be found")
	}

	if dc.Router != "round-robin-pool" {
		t.Errorf("Router = %q, want %q", dc.Router, "round-robin-pool")
	}
	if dc.NrOfInstances != 3 {
		t.Errorf("NrOfInstances = %d, want 3", dc.NrOfInstances)
	}
	if !dc.Resizer.Enabled {
		t.Fatal("expected Resizer.Enabled = true")
	}
	if dc.Resizer.LowerBound != 2 {
		t.Errorf("LowerBound = %d, want 2", dc.Resizer.LowerBound)
	}
	if dc.Resizer.UpperBound != 15 {
		t.Errorf("UpperBound = %d, want 15", dc.Resizer.UpperBound)
	}
	if dc.Resizer.PressureThreshold != 3 {
		t.Errorf("PressureThreshold = %d, want 3", dc.Resizer.PressureThreshold)
	}
	if dc.Resizer.RampupRate != 0.25 {
		t.Errorf("RampupRate = %f, want 0.25", dc.Resizer.RampupRate)
	}
	if dc.Resizer.BackoffRate != 0.1 {
		t.Errorf("BackoffRate = %f, want 0.1", dc.Resizer.BackoffRate)
	}
	if dc.Resizer.BackoffThreshold != 0.4 {
		t.Errorf("BackoffThreshold = %f, want 0.4", dc.Resizer.BackoffThreshold)
	}
	if dc.Resizer.MessagesPerResize != 20 {
		t.Errorf("MessagesPerResize = %d, want 20", dc.Resizer.MessagesPerResize)
	}
}

func TestDeploymentConfig_ResizerDefaults(t *testing.T) {
	input := `
pekko.actor.deployment {
  "/user/defaultPool" {
    router = round-robin-pool
    nr-of-instances = 5
    resizer {}
  }
}
`
	cfg, err := hocon.ParseString(input)
	if err != nil {
		t.Fatalf("failed to parse HOCON: %v", err)
	}

	dc, ok := LookupDeployment(cfg, "/user/defaultPool")
	if !ok {
		t.Fatal("expected deployment config to be found")
	}

	if !dc.Resizer.Enabled {
		t.Fatal("expected Resizer.Enabled = true for empty resizer block")
	}
	if dc.Resizer.LowerBound != 1 {
		t.Errorf("LowerBound = %d, want 1 (default)", dc.Resizer.LowerBound)
	}
	if dc.Resizer.UpperBound != 10 {
		t.Errorf("UpperBound = %d, want 10 (default)", dc.Resizer.UpperBound)
	}
	if dc.Resizer.PressureThreshold != 1 {
		t.Errorf("PressureThreshold = %d, want 1 (default)", dc.Resizer.PressureThreshold)
	}
	if dc.Resizer.RampupRate != 0.2 {
		t.Errorf("RampupRate = %f, want 0.2 (default)", dc.Resizer.RampupRate)
	}
	if dc.Resizer.BackoffRate != 0.3 {
		t.Errorf("BackoffRate = %f, want 0.3 (default)", dc.Resizer.BackoffRate)
	}
	if dc.Resizer.BackoffThreshold != 0.3 {
		t.Errorf("BackoffThreshold = %f, want 0.3 (default)", dc.Resizer.BackoffThreshold)
	}
	if dc.Resizer.MessagesPerResize != 10 {
		t.Errorf("MessagesPerResize = %d, want 10 (default)", dc.Resizer.MessagesPerResize)
	}
}

func TestDeploymentConfig_NoResizer(t *testing.T) {
	input := `
pekko.actor.deployment {
  "/user/plain" {
    router = round-robin-pool
    nr-of-instances = 5
  }
}
`
	cfg, err := hocon.ParseString(input)
	if err != nil {
		t.Fatalf("failed to parse HOCON: %v", err)
	}

	dc, ok := LookupDeployment(cfg, "/user/plain")
	if !ok {
		t.Fatal("expected deployment config to be found")
	}

	if dc.Resizer.Enabled {
		t.Error("expected Resizer.Enabled = false when no resizer block")
	}
}

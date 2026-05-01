/*
 * hocon_config_watch_fd_test.go
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

// Verifies pekko.remote.watch-failure-detector.* keys parse into the
// WatchFailureDetector ClusterConfig field.
func TestHOCON_WatchFailureDetector_AllKeysParsed(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote {
    artery { canonical { hostname = "127.0.0.1", port = 2552 } }
    watch-failure-detector {
      implementation-class               = "com.example.MyDetector"
      heartbeat-interval                 = 2s
      threshold                          = 7.5
      max-sample-size                    = 250
      min-std-deviation                  = 75ms
      acceptable-heartbeat-pause         = 4s
      unreachable-nodes-reaper-interval  = 500ms
      expected-response-after            = 800ms
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	w := cfg.WatchFailureDetector
	if w.ImplementationClass != "com.example.MyDetector" {
		t.Errorf("ImplementationClass = %q, want \"com.example.MyDetector\"", w.ImplementationClass)
	}
	if w.HeartbeatInterval != 2*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 2s", w.HeartbeatInterval)
	}
	if w.Threshold != 7.5 {
		t.Errorf("Threshold = %v, want 7.5", w.Threshold)
	}
	if w.MaxSampleSize != 250 {
		t.Errorf("MaxSampleSize = %d, want 250", w.MaxSampleSize)
	}
	if w.MinStdDeviation != 75*time.Millisecond {
		t.Errorf("MinStdDeviation = %v, want 75ms", w.MinStdDeviation)
	}
	if w.AcceptableHeartbeatPause != 4*time.Second {
		t.Errorf("AcceptableHeartbeatPause = %v, want 4s", w.AcceptableHeartbeatPause)
	}
	if w.UnreachableNodesReaperInterval != 500*time.Millisecond {
		t.Errorf("UnreachableNodesReaperInterval = %v, want 500ms", w.UnreachableNodesReaperInterval)
	}
	if w.ExpectedResponseAfter != 800*time.Millisecond {
		t.Errorf("ExpectedResponseAfter = %v, want 800ms", w.ExpectedResponseAfter)
	}
}

// Verifies the documented Pekko reference defaults are applied when the
// pekko.remote.watch-failure-detector namespace is omitted.
func TestHOCON_WatchFailureDetector_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	w := cfg.WatchFailureDetector
	if w.ImplementationClass != "org.apache.pekko.remote.PhiAccrualFailureDetector" {
		t.Errorf("ImplementationClass = %q, want Pekko default", w.ImplementationClass)
	}
	if w.HeartbeatInterval != 1*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 1s", w.HeartbeatInterval)
	}
	if w.Threshold != 10.0 {
		t.Errorf("Threshold = %v, want 10.0", w.Threshold)
	}
	if w.MaxSampleSize != 200 {
		t.Errorf("MaxSampleSize = %d, want 200", w.MaxSampleSize)
	}
	if w.MinStdDeviation != 100*time.Millisecond {
		t.Errorf("MinStdDeviation = %v, want 100ms", w.MinStdDeviation)
	}
	if w.AcceptableHeartbeatPause != 10*time.Second {
		t.Errorf("AcceptableHeartbeatPause = %v, want 10s", w.AcceptableHeartbeatPause)
	}
	if w.UnreachableNodesReaperInterval != 1*time.Second {
		t.Errorf("UnreachableNodesReaperInterval = %v, want 1s", w.UnreachableNodesReaperInterval)
	}
	if w.ExpectedResponseAfter != 1*time.Second {
		t.Errorf("ExpectedResponseAfter = %v, want 1s", w.ExpectedResponseAfter)
	}
}

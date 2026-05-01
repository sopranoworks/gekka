/*
 * external_strategy_clienttimeout_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	config "github.com/sopranoworks/gekka-config"
)

// TestLoadAllocationStrategy_ExternalClientTimeout_FromCanonicalPekkoPath
// verifies the canonical Pekko path
// pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout
// is honored by LoadAllocationStrategy when the gekka-native
// gekka.cluster.sharding.allocation-strategy.external.timeout is unset.
//
// Runtime check: stand up an HTTP server that hangs longer than the
// configured timeout but well under the 5s Pekko default. The strategy must
// give up within the configured budget and fall back, proving the canonical
// client-timeout drives the http.Client at runtime.
func TestLoadAllocationStrategy_ExternalClientTimeout_FromCanonicalPekkoPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"region":"unused"}`))
	}))
	t.Cleanup(server.Close)

	cfg, err := config.ParseString(`
gekka.cluster.sharding.allocation-strategy {
  type = "external"
  external.url = "` + server.URL + `"
}
pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout = 120ms
`)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}

	fallback := NewLeastShardAllocationStrategy(1, 1)
	strategy := LoadAllocationStrategy(*cfg, nil, fallback)

	ext, ok := strategy.(*ExternalShardAllocationStrategy)
	if !ok {
		t.Fatalf("LoadAllocationStrategy returned %T, want *ExternalShardAllocationStrategy", strategy)
	}
	if got, want := ext.timeout, 120*time.Millisecond; got != want {
		t.Fatalf("ext.timeout = %v, want %v (canonical Pekko client-timeout)", got, want)
	}

	requester := &mockRef{path: "/user/req"}
	start := time.Now()
	_ = ext.AllocateShard(requester, "shard-1", nil)
	elapsed := time.Since(start)
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("AllocateShard elapsed=%v — client timeout did not fire within budget; "+
			"canonical client-timeout=120ms appears not to be honored", elapsed)
	}
}

// TestLoadAllocationStrategy_ExternalClientTimeout_GekkaNativeWins verifies
// the gekka-native external.timeout takes precedence when both are set.
func TestLoadAllocationStrategy_ExternalClientTimeout_GekkaNativeWins(t *testing.T) {
	cfg, err := config.ParseString(`
gekka.cluster.sharding.allocation-strategy {
  type = "external"
  external.url = "http://127.0.0.1:1"
  external.timeout = 333ms
}
pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout = 7s
`)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	strategy := LoadAllocationStrategy(*cfg, nil, NewLeastShardAllocationStrategy(1, 1))
	ext, ok := strategy.(*ExternalShardAllocationStrategy)
	if !ok {
		t.Fatalf("LoadAllocationStrategy returned %T", strategy)
	}
	if got, want := ext.timeout, 333*time.Millisecond; got != want {
		t.Fatalf("ext.timeout = %v, want %v (gekka-native must win over canonical)", got, want)
	}
}

// TestLoadAllocationStrategy_ExternalClientTimeout_DefaultIsFiveSeconds
// guards the Pekko default when neither path is set.
func TestLoadAllocationStrategy_ExternalClientTimeout_DefaultIsFiveSeconds(t *testing.T) {
	cfg, err := config.ParseString(`
gekka.cluster.sharding.allocation-strategy {
  type = "external"
  external.url = "http://127.0.0.1:1"
}
`)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	strategy := LoadAllocationStrategy(*cfg, nil, NewLeastShardAllocationStrategy(1, 1))
	ext, ok := strategy.(*ExternalShardAllocationStrategy)
	if !ok {
		t.Fatalf("LoadAllocationStrategy returned %T", strategy)
	}
	if got, want := ext.timeout, 5*time.Second; got != want {
		t.Fatalf("ext.timeout = %v, want %v (Pekko default)", got, want)
	}
}

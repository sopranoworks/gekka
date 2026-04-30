/*
 * hocon_config_discovery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"sort"
	"testing"
)

func TestHOCON_DiscoveryConfigNamespace(t *testing.T) {
	const text = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2551 }
  discovery {
    method = "config"
    config {
      service-name = "svc"
      default-port = 2551
      services {
        svc {
          endpoints = [ "host1:1233", "host2" ]
        }
      }
    }
  }
}
`
	cfg, err := parseHOCONString(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cfg.Discovery.Enabled {
		t.Fatal("Discovery.Enabled = false, want true")
	}
	if cfg.Discovery.Type != "config" {
		t.Errorf("Discovery.Type = %q, want config", cfg.Discovery.Type)
	}
	if got, _ := cfg.Discovery.Config.Config["service-name"].(string); got != "svc" {
		t.Errorf("service-name = %q, want svc", got)
	}
	if got, _ := cfg.Discovery.Config.Config["default-port"].(int); got != 2551 {
		t.Errorf("default-port = %d, want 2551", got)
	}
	servicesAny, ok := cfg.Discovery.Config.Config["services"]
	if !ok {
		t.Fatal("services key missing from Discovery.Config.Config")
	}
	services, ok := servicesAny.(map[string]any)
	if !ok {
		t.Fatalf("services has wrong type: %T", servicesAny)
	}
	svcAny, ok := services["svc"]
	if !ok {
		t.Fatal("services.svc missing")
	}
	svc, _ := svcAny.(map[string]any)
	endpointsAny, _ := svc["endpoints"]
	endpoints, ok := endpointsAny.([]any)
	if !ok {
		t.Fatalf("endpoints has wrong type: %T", endpointsAny)
	}
	got := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		got = append(got, e.(string))
	}
	sort.Strings(got)
	want := []string{"host1:1233", "host2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("endpoints = %v, want %v", got, want)
	}
}

func TestHOCON_DiscoveryConfigCustomServicesPath(t *testing.T) {
	const text = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2551 }
  discovery {
    method = "config"
    config {
      service-name = "svc"
      services-path = "myapp.discovery.services"
    }
  }
}
myapp.discovery.services {
  svc {
    endpoints = [ "h1:1" ]
  }
}
`
	cfg, err := parseHOCONString(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	servicesAny, ok := cfg.Discovery.Config.Config["services"]
	if !ok {
		t.Fatal("services key missing — services-path indirection not honoured")
	}
	services, _ := servicesAny.(map[string]any)
	if _, ok := services["svc"]; !ok {
		t.Errorf("services map = %v, want key 'svc'", services)
	}
}

func TestHOCON_DiscoveryAggregateNamespace(t *testing.T) {
	const text = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2551 }
  discovery {
    method = "aggregate"
    aggregate {
      discovery-methods = [ "config", "pekko-dns" ]
    }
  }
}
`
	cfg, err := parseHOCONString(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Discovery.Type != "aggregate" {
		t.Errorf("Discovery.Type = %q, want aggregate", cfg.Discovery.Type)
	}
	dmAny, ok := cfg.Discovery.Config.Config["discovery-methods"]
	if !ok {
		t.Fatal("discovery-methods key missing")
	}
	dm, ok := dmAny.([]string)
	if !ok {
		t.Fatalf("discovery-methods has wrong type: %T", dmAny)
	}
	want := []string{"config", "pekko-dns"}
	if len(dm) != len(want) || dm[0] != want[0] || dm[1] != want[1] {
		t.Errorf("discovery-methods = %v, want %v", dm, want)
	}
}

func TestHOCON_DiscoveryPekkoDNSNamespace(t *testing.T) {
	const text = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2551 }
  discovery {
    method = "pekko-dns"
    pekko-dns {
      service-name = "_artery._tcp.svc.local"
      port = 2551
    }
  }
}
`
	cfg, err := parseHOCONString(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Discovery.Type != "pekko-dns" {
		t.Errorf("Discovery.Type = %q, want pekko-dns", cfg.Discovery.Type)
	}
	if got, _ := cfg.Discovery.Config.Config["service-name"].(string); got != "_artery._tcp.svc.local" {
		t.Errorf("service-name = %q", got)
	}
	if got, _ := cfg.Discovery.Config.Config["port"].(int); got != 2551 {
		t.Errorf("port = %d", got)
	}
}

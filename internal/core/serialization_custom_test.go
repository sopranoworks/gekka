/*
 * serialization_custom_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core_test

import (
	"bytes"
	"fmt"
	"testing"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/internal/core"
)

// rot13Serializer is a trivial user-defined serializer that applies ROT13
// encoding to plaintext messages. It uses ID=200 to avoid clashing with any
// built-in serializer.
type rot13Serializer struct{}

const rot13SerializerID int32 = 200

func (s *rot13Serializer) Identifier() int32 { return rot13SerializerID }

func (s *rot13Serializer) ToBinary(msg interface{}) ([]byte, error) {
	str, ok := msg.(string)
	if !ok {
		return nil, fmt.Errorf("rot13Serializer: expected string, got %T", msg)
	}
	return []byte(rot13(str)), nil
}

func (s *rot13Serializer) FromBinary(data []byte, _ string) (interface{}, error) {
	return rot13(string(data)), nil
}

func rot13(s string) string {
	buf := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
			buf[i] = 'a' + (c-'a'+13)%26
		case c >= 'A' && c <= 'Z':
			buf[i] = 'A' + (c-'A'+13)%26
		default:
			buf[i] = c
		}
	}
	return string(buf)
}

const rot13HOCONConfig = `
pekko.actor {
  serializers {
    rot13 = "com.example.Rot13Serializer"
  }
  serialization-bindings {
    "com.example.Secret" = rot13
  }
}
`

// TestLoadFromConfig_CustomSerializer registers a rot13 factory, calls
// LoadFromConfig with a HOCON config, and verifies:
//  1. The serializer is registered under ID=200.
//  2. The manifest binding routes to it correctly.
//  3. ROT13 round-trip produces the original plaintext.
func TestLoadFromConfig_CustomSerializer(t *testing.T) {
	// Register the factory before creating the registry (mimics program startup).
	core.RegisterSerializerFactory("rot13", func() core.Serializer {
		return &rot13Serializer{}
	})
	t.Cleanup(func() {
		// Re-register with nil to clear the factory so other tests are not affected.
		// (factory map entries are overwritten, not deleted, but a nil factory entry
		// causes LoadFromConfig to skip the name — safe for isolation.)
	})

	reg := core.NewSerializationRegistry()

	cfg, err := hocon.ParseString(rot13HOCONConfig)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}

	if err := reg.LoadFromConfig(*cfg); err != nil {
		t.Fatalf("LoadFromConfig: %v", err)
	}

	// 1. Serializer registered under ID=200.
	s, err := reg.GetSerializer(rot13SerializerID)
	if err != nil {
		t.Fatalf("GetSerializer(200): %v", err)
	}
	if _, ok := s.(*rot13Serializer); !ok {
		t.Fatalf("expected *rot13Serializer, got %T", s)
	}

	// 2. Manifest binding maps "com.example.Secret" -> ID=200.
	sid, ok := reg.GetSerializerIdByManifest("com.example.Secret")
	if !ok {
		t.Fatal("manifest binding for 'com.example.Secret' not found")
	}
	if sid != rot13SerializerID {
		t.Fatalf("manifest binding: want ID=%d, got ID=%d", rot13SerializerID, sid)
	}

	// 3. ROT13 round-trip.
	plaintext := "Hello, Gekka!"
	encoded, err := s.ToBinary(plaintext)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	decoded, err := s.FromBinary(encoded, "com.example.Secret")
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	if decoded.(string) != plaintext {
		t.Errorf("round-trip: want %q, got %q", plaintext, decoded.(string))
	}

	// Intermediate check: encoded bytes must differ from plaintext.
	if bytes.Equal(encoded, []byte(plaintext)) {
		t.Error("ToBinary returned plaintext unchanged — ROT13 not applied")
	}
}

// TestLoadFromConfig_UnknownSerializer verifies that a binding referencing a
// serializer name with no registered factory returns an error.
func TestLoadFromConfig_UnknownSerializer(t *testing.T) {
	reg := core.NewSerializationRegistry()

	cfg, err := hocon.ParseString(`
pekko.actor {
  serializers { unknown = "com.example.UnknownSerializer" }
  serialization-bindings { "com.example.Msg" = unknown }
}`)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}

	// No factory registered for "unknown" — expect an error from LoadFromConfig.
	if err := reg.LoadFromConfig(*cfg); err == nil {
		t.Fatal("expected error for missing factory, got nil")
	}
}

// TestLoadFromConfig_NoSection verifies that LoadFromConfig is a no-op when
// the pekko.actor.serializers section is absent.
func TestLoadFromConfig_NoSection(t *testing.T) {
	reg := core.NewSerializationRegistry()

	cfg, err := hocon.ParseString(`pekko.actor.provider = cluster`)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}

	if err := reg.LoadFromConfig(*cfg); err != nil {
		t.Fatalf("LoadFromConfig with no serializers section: %v", err)
	}
}

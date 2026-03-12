/*
 * serialization_registry_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"encoding/json"
	"fmt"
	"testing"
	"github.com/sopranoworks/gekka/internal/core"
)

// ── Dummy JSON Serializer (ID 100) ────────────────────────────────────────────
//
// A minimal example of a custom Serializer that encodes Go structs as JSON and
// uses the Artery manifest string to select the target type on deserialization.

// orderPlaced is a sample application event used in the tests below.
type orderPlaced struct {
	OrderID  string  `json:"order_id"`
	Product  string  `json:"product"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// userCreated is a second event type to exercise manifest dispatch.
type userCreated struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

// dummyJSONSerializer encodes any value as JSON and dispatches deserialization
// by manifest string — mirroring how a Jackson-based Pekko serializer works.
type dummyJSONSerializer struct{}

func (d *dummyJSONSerializer) Identifier() int32 { return 100 }

func (d *dummyJSONSerializer) ToBinary(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

func (d *dummyJSONSerializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	switch manifest {
	case "orderPlaced":
		var e orderPlaced
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, err
		}
		return e, nil
	case "userCreated":
		var u userCreated
		if err := json.Unmarshal(data, &u); err != nil {
			return nil, err
		}
		return u, nil
	default:
		return nil, fmt.Errorf("dummyJSONSerializer: unknown manifest %q", manifest)
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestSerializationRegistry_RegisterAndRetrieve(t *testing.T) {
	reg := core.NewSerializationRegistry()

	// Verify default serializers are present.
	if _, err := reg.GetSerializer(2); err != nil {
		t.Fatalf("expected built-in serializer for ID 2: %v", err)
	}
	if _, err := reg.GetSerializer(4); err != nil {
		t.Fatalf("expected built-in serializer for ID 4: %v", err)
	}

	// Register custom serializer.
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	s, err := reg.GetSerializer(100)
	if err != nil {
		t.Fatalf("GetSerializer(100): %v", err)
	}
	if s.Identifier() != 100 {
		t.Errorf("Identifier(): got %d, want 100", s.Identifier())
	}

	// Unknown ID must return an error.
	if _, err := reg.GetSerializer(999); err == nil {
		t.Error("expected error for unregistered ID 999, got nil")
	}
}

func TestSerializationRegistry_ByteArraySerializer_RoundTrip(t *testing.T) {
	reg := core.NewSerializationRegistry()

	original := []byte("raw artery payload \x00\x01\x02")

	// ToBinary
	s, _ := reg.GetSerializer(4)
	encoded, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	if string(encoded) != string(original) {
		t.Errorf("ToBinary: bytes changed; got %x, want %x", encoded, original)
	}

	// FromBinary (manifest is ignored for ByteArraySerializer)
	decoded, err := reg.DeserializePayload(4, "", encoded)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}
	got, ok := decoded.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", decoded)
	}
	if string(got) != string(original) {
		t.Errorf("round-trip mismatch: got %x, want %x", got, original)
	}
}

func TestSerializationRegistry_ByteArraySerializer_NonBytesError(t *testing.T) {
	reg := core.NewSerializationRegistry()
	s, _ := reg.GetSerializer(4)
	if _, err := s.ToBinary("not a []byte"); err == nil {
		t.Error("expected error when ToBinary receives a string, got nil")
	}
}

func TestSerializationRegistry_CustomJSON_ToBinary(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	event := orderPlaced{OrderID: "ORD-1", Product: "widget", Quantity: 3, Price: 9.99}

	s, _ := reg.GetSerializer(100)
	data, err := s.ToBinary(event)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("ToBinary returned empty bytes")
	}

	// Sanity-check the JSON structure.
	var check map[string]interface{}
	if err := json.Unmarshal(data, &check); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if check["order_id"] != "ORD-1" {
		t.Errorf("JSON order_id: got %v, want ORD-1", check["order_id"])
	}
}

func TestSerializationRegistry_CustomJSON_RoundTrip_OrderPlaced(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	original := orderPlaced{OrderID: "ORD-42", Product: "gadget", Quantity: 7, Price: 19.95}

	s, _ := reg.GetSerializer(100)
	data, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	decoded, err := reg.DeserializePayload(100, "orderPlaced", data)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}
	got, ok := decoded.(orderPlaced)
	if !ok {
		t.Fatalf("expected orderPlaced, got %T", decoded)
	}
	if got != original {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", got, original)
	}
}

func TestSerializationRegistry_CustomJSON_RoundTrip_UserCreated(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	original := userCreated{UserID: "u-99", Email: "test@example.com"}

	s, _ := reg.GetSerializer(100)
	data, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	decoded, err := reg.DeserializePayload(100, "userCreated", data)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}
	got, ok := decoded.(userCreated)
	if !ok {
		t.Fatalf("expected userCreated, got %T", decoded)
	}
	if got != original {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", got, original)
	}
}

func TestSerializationRegistry_CustomJSON_UnknownManifest(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	data := []byte(`{"foo":"bar"}`)
	if _, err := reg.DeserializePayload(100, "unknownType", data); err == nil {
		t.Error("expected error for unknown manifest, got nil")
	}
}

func TestSerializationRegistry_OverrideBuiltin(t *testing.T) {
	// RegisterSerializer with an explicit id must allow overriding built-ins.
	reg := core.NewSerializationRegistry()

	// Override ID 4 with a custom implementation.
	reg.RegisterSerializer(4, &dummyJSONSerializer{})

	s, _ := reg.GetSerializer(4)
	if s.Identifier() != 100 {
		// dummyJSONSerializer.Identifier() returns 100 even though it was
		// registered under ID 4 — confirming the explicit id wins at lookup
		// time while the interface Identifier() still reports 100.
		t.Errorf("Identifier(): got %d, want 100", s.Identifier())
	}
}

func TestSerializationRegistry_ManifestDispatch(t *testing.T) {
	// Verify that the same serializer correctly dispatches two different
	// manifest strings to two different Go structs.
	reg := core.NewSerializationRegistry()
	reg.RegisterSerializer(100, &dummyJSONSerializer{})

	s, _ := reg.GetSerializer(100)

	order := orderPlaced{OrderID: "O1", Product: "p", Quantity: 1, Price: 1.0}
	user := userCreated{UserID: "U1", Email: "u@example.com"}

	orderBytes, _ := s.ToBinary(order)
	userBytes, _ := s.ToBinary(user)

	decodedOrder, err := reg.DeserializePayload(100, "orderPlaced", orderBytes)
	if err != nil {
		t.Fatalf("DeserializePayload orderPlaced: %v", err)
	}
	if _, ok := decodedOrder.(orderPlaced); !ok {
		t.Errorf("expected orderPlaced, got %T", decodedOrder)
	}

	decodedUser, err := reg.DeserializePayload(100, "userCreated", userBytes)
	if err != nil {
		t.Fatalf("DeserializePayload userCreated: %v", err)
	}
	if _, ok := decodedUser.(userCreated); !ok {
		t.Errorf("expected userCreated, got %T", decodedUser)
	}
}

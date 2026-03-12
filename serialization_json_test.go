/*
 * serialization_json_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
)

// ── shared domain types ────────────────────────────────────────────────────────

// shippingEvent is a sample domain event used in all JSON-serialization tests.
type shippingEvent struct {
	OrderID   string  `json:"order_id"`
	Warehouse string  `json:"warehouse"`
	Qty       int     `json:"qty"`
	Weight    float64 `json:"weight_kg"`
}

// paymentEvent is a second type to verify manifest dispatch.
type paymentEvent struct {
	TxID   string  `json:"tx_id"`
	Amount float64 `json:"amount"`
}

const (
	manifestShipping = "com.example.ShippingEvent"
	manifestPayment  = "com.example.PaymentEvent"
)

// ── JSONSerializer unit tests ─────────────────────────────────────────────────

func TestJSONSerializer_IsRegisteredByDefault(t *testing.T) {
	reg := core.NewSerializationRegistry()
	s, err := reg.GetSerializer(actor.JSONSerializerID)
	if err != nil {
		t.Fatalf("JSONSerializer not registered by default: %v", err)
	}
	if s.Identifier() != actor.JSONSerializerID {
		t.Errorf("Identifier() = %d, want %d", s.Identifier(), actor.JSONSerializerID)
	}
}

func TestJSONSerializer_ToBinary_ProducesValidJSON(t *testing.T) {
	reg := core.NewSerializationRegistry()
	s, _ := reg.GetSerializer(actor.JSONSerializerID)

	event := shippingEvent{OrderID: "ORD-1", Warehouse: "WH-A", Qty: 3, Weight: 4.5}
	data, err := s.ToBinary(event)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("ToBinary returned empty bytes")
	}
	// Quick sanity-check: valid JSON should contain the field value.
	if string(data) == "" || !contains(string(data), "ORD-1") {
		t.Errorf("JSON output does not contain expected field: %s", data)
	}
}

func TestJSONSerializer_RoundTrip_ValueType(t *testing.T) {
	reg := core.NewSerializationRegistry()
	// Register with value type (not pointer).
	reg.RegisterManifest(manifestShipping, reflect.TypeOf(shippingEvent{}))

	s, _ := reg.GetSerializer(actor.JSONSerializerID)

	original := shippingEvent{OrderID: "ORD-99", Warehouse: "WH-B", Qty: 10, Weight: 9.81}
	data, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	got, err := reg.DeserializePayload(actor.JSONSerializerID, manifestShipping, data)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}
	evt, ok := got.(shippingEvent)
	if !ok {
		t.Fatalf("expected shippingEvent, got %T", got)
	}
	if evt != original {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", evt, original)
	}
}

func TestJSONSerializer_RoundTrip_PointerType(t *testing.T) {
	reg := core.NewSerializationRegistry()
	// Register with pointer type.
	reg.RegisterManifest(manifestPayment, reflect.TypeOf((*paymentEvent)(nil)))

	s, _ := reg.GetSerializer(actor.JSONSerializerID)

	original := &paymentEvent{TxID: "TX-42", Amount: 199.99}
	data, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	got, err := reg.DeserializePayload(actor.JSONSerializerID, manifestPayment, data)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}
	evt, ok := got.(*paymentEvent)
	if !ok {
		t.Fatalf("expected *paymentEvent, got %T", got)
	}
	if *evt != *original {
		t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", *evt, *original)
	}
}

func TestJSONSerializer_UnknownManifest_ReturnsError(t *testing.T) {
	reg := core.NewSerializationRegistry()
	_, err := reg.DeserializePayload(actor.JSONSerializerID, "com.example.Unknown", []byte(`{}`))
	if err == nil {
		t.Error("expected error for unknown manifest, got nil")
	}
}

func TestJSONSerializer_InvalidJSON_ReturnsError(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterManifest(manifestShipping, reflect.TypeOf(shippingEvent{}))

	_, err := reg.DeserializePayload(actor.JSONSerializerID, manifestShipping, []byte(`not-json{`))
	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}

func TestJSONSerializer_ManifestDispatch_TwoTypes(t *testing.T) {
	reg := core.NewSerializationRegistry()
	reg.RegisterManifest(manifestShipping, reflect.TypeOf(shippingEvent{}))
	reg.RegisterManifest(manifestPayment, reflect.TypeOf(paymentEvent{}))

	s, _ := reg.GetSerializer(actor.JSONSerializerID)

	ship := shippingEvent{OrderID: "S1", Warehouse: "W", Qty: 1, Weight: 0.5}
	pay := paymentEvent{TxID: "T1", Amount: 50.0}

	shipBytes, _ := s.ToBinary(ship)
	payBytes, _ := s.ToBinary(pay)

	gotShip, err := reg.DeserializePayload(actor.JSONSerializerID, manifestShipping, shipBytes)
	if err != nil {
		t.Fatalf("shipping dispatch: %v", err)
	}
	if _, ok := gotShip.(shippingEvent); !ok {
		t.Errorf("expected shippingEvent, got %T", gotShip)
	}

	gotPay, err := reg.DeserializePayload(actor.JSONSerializerID, manifestPayment, payBytes)
	if err != nil {
		t.Fatalf("payment dispatch: %v", err)
	}
	if _, ok := gotPay.(paymentEvent); !ok {
		t.Errorf("expected paymentEvent, got %T", gotPay)
	}
}

// ── GekkaNode convenience methods ─────────────────────────────────────────────

func TestGekkaNode_RegisterType_AndRetrieve(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	node.nm.SerializerRegistry = core.NewSerializationRegistry()

	node.RegisterType(manifestShipping, reflect.TypeOf(shippingEvent{}))

	typ, ok := node.Serialization().GetTypeByManifest(manifestShipping)
	if !ok {
		t.Fatal("RegisterType: manifest not found in registry")
	}
	if typ != reflect.TypeOf(shippingEvent{}) {
		t.Errorf("RegisterType: got %v, want %v", typ, reflect.TypeOf(shippingEvent{}))
	}
}

func TestGekkaNode_RegisterSerializer_OverridesExisting(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	node.nm.SerializerRegistry = core.NewSerializationRegistry()

	// Replace JSON serializer with a custom one keyed by its identifier.
	custom := &customTestSerializer{}
	node.RegisterSerializer(custom)

	got, err := node.Serialization().GetSerializer(custom.Identifier())
	if err != nil {
		t.Fatalf("GetSerializer: %v", err)
	}
	if _, ok := got.(*customTestSerializer); !ok {
		t.Errorf("expected *customTestSerializer, got %T", got)
	}
}

// customTestSerializer is a no-op serializer used in the override test.
type customTestSerializer struct{}

func (c *customTestSerializer) Identifier() int32                                  { return 42 }
func (c *customTestSerializer) ToBinary(_ interface{}) ([]byte, error)             { return nil, nil }
func (c *customTestSerializer) FromBinary(_ []byte, _ string) (interface{}, error) { return nil, nil }

// ── Actor integration test ────────────────────────────────────────────────────
//
// This test verifies the end-to-end pipeline for a Go struct delivered to a
// local actor. Local delivery places the value directly into the mailbox (no
// Artery serialization). The test also exercises the JSONSerializer round-trip
// to prove the framework can reconstruct the struct from bytes — simulating
// what happens when the same message crosses the Artery wire.

// eventCapture records messages received inside Receive.
type eventCapture struct {
	actor.BaseActor
	received chan any
}

func (a *eventCapture) Receive(msg any) {
	select {
	case a.received <- msg:
	default:
	}
}

func TestJSONSerializer_ActorReceivesGoStruct_LocalDelivery(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	node.nm.SerializerRegistry = core.NewSerializationRegistry()
	node.RegisterType(manifestShipping, reflect.TypeOf(shippingEvent{}))

	cap := &eventCapture{
		BaseActor: actor.NewBaseActor(),
		received:  make(chan any, 1),
	}
	ref := node.SpawnActor("/user/captor", cap, actor.Props{New: func() actor.Actor { return cap }})

	// Deliver a struct directly — local mailbox bypasses the serializer.
	event := shippingEvent{OrderID: "E-001", Warehouse: "W1", Qty: 5, Weight: 2.3}
	ref.Tell(event)

	select {
	case msg := <-cap.received:
		got, ok := msg.(shippingEvent)
		if !ok {
			t.Fatalf("actor received %T, want shippingEvent", msg)
		}
		if got != event {
			t.Errorf("received %+v, want %+v", got, event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("actor did not receive message within 500ms")
	}
}

func TestJSONSerializer_ActorReceivesGoStruct_ViaSerializer(t *testing.T) {
	// Simulate what Artery does: serialize then deserialize using JSONSerializer.
	// The actor then receives the reconstructed value.
	node := newTestNode(t, "Sys", "127.0.0.1", 2552)
	reg := node.nm.SerializerRegistry
	reg.RegisterManifest(manifestShipping, reflect.TypeOf(shippingEvent{}))

	cap := &eventCapture{
		BaseActor: actor.NewBaseActor(),
		received:  make(chan any, 1),
	}
	ref := node.SpawnActor("/user/captor", cap, actor.Props{New: func() actor.Actor { return cap }})

	// --- Sender side: serialize ---
	original := shippingEvent{OrderID: "E-002", Warehouse: "W2", Qty: 2, Weight: 1.1}
	s, err := reg.GetSerializer(actor.JSONSerializerID)
	if err != nil {
		t.Fatalf("GetSerializer: %v", err)
	}
	wireBytes, err := s.ToBinary(original)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	// --- Receiver side: deserialize (as Artery's handleUserMessage would) ---
	decoded, err := reg.DeserializePayload(actor.JSONSerializerID, manifestShipping, wireBytes)
	if err != nil {
		t.Fatalf("DeserializePayload: %v", err)
	}

	// Deliver the deserialized value to the actor.
	ref.Tell(decoded)

	select {
	case msg := <-cap.received:
		got, ok := msg.(shippingEvent)
		if !ok {
			t.Fatalf("actor received %T, want shippingEvent", msg)
		}
		if got != original {
			t.Errorf("received %+v, want %+v", got, original)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("actor did not receive message within 500ms")
	}
}

// ── helper ────────────────────────────────────────────────────────────────────

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsRune(s, sub))
}

func containsRune(s, sub string) bool {
	for i := range s {
		if i+len(sub) <= len(s) && s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

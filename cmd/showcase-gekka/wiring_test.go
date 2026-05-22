// cmd/showcase-gekka/wiring_test.go — Phase 3 root-cause isolation.
//
// Validates that after RegisterShowcaseTypes + registry.RegisterManifest,
// SerializePayload(&EchoEnvelope{...}) routes through JacksonCborSerializer
// (sid=33) with the JVM manifest, NOT through JSONSerializer (sid=9) with
// the Go reflect-type fallback manifest.
//
// SPDX-License-Identifier: MIT
package main

import (
	"reflect"
	"testing"

	"github.com/sopranoworks/gekka/internal/core"
	jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"
)

func TestWiring_EchoEnvelope_RoutesToJacksonCbor(t *testing.T) {
	registry := core.NewSerializationRegistry()
	ser := jcbor.New(jcbor.DefaultID)
	RegisterShowcaseTypes(ser)
	registry.RegisterSerializer(ser.Identifier(), ser)

	registry.RegisterManifest(JVMEchoEnvelope, reflect.TypeOf((*EchoEnvelope)(nil)), jcbor.DefaultID)

	env := NewEchoEnvelope(1, "g1", "SEND", "string", "hello")
	payload, sid, manifest, err := registry.SerializePayload(env)
	if err != nil {
		t.Fatalf("SerializePayload: %v", err)
	}
	if sid != jcbor.DefaultID {
		t.Errorf("sid: got %d, want %d (JacksonCbor)", sid, jcbor.DefaultID)
	}
	if manifest != JVMEchoEnvelope {
		t.Errorf("manifest: got %q, want %q", manifest, JVMEchoEnvelope)
	}
	if len(payload) == 0 {
		t.Error("payload is empty")
	}
	t.Logf("OK: sid=%d, manifest=%q, len=%d", sid, manifest, len(payload))
}

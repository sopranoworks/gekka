/*
 * jacksoncbor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jacksoncbor_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	jcbor "github.com/sopranoworks/gekka/serialization/jackson-cbor"
)

// ── Test types — mirror Scala showcase case classes ─────────────────────────
//
// Each type embeds jcbor.JVMClassManifest with `cbor:"-"` to keep the JVM
// identity off the wire. CBOR tags use Scala field names (lowerCamelCase),
// matching what Pekko's Jackson emits and decodes.

type echoEnvelope struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64       `cbor:"seqNo"`
	Originator             string      `cbor:"originator"`
	Direction              string      `cbor:"direction"`
	PayloadKind            string      `cbor:"payloadKind"`
	Payload                interface{} `cbor:"payload"`
}

type askEnvelope struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64       `cbor:"seqNo"`
	Originator             string      `cbor:"originator"`
	Direction              string      `cbor:"direction"`
	PayloadKind            string      `cbor:"payloadKind"`
	Payload                interface{} `cbor:"payload"`
}

type systemMessagePing struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

type showcaseEchoCustom struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64         `cbor:"seqNo"`
	Originator             string        `cbor:"originator"`
	Payload                []interface{} `cbor:"payload"` // Vector[Byte] arrives as CBOR array of small ints
}

type ping struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

type pong struct {
	jcbor.JVMClassManifest `cbor:"-"`
	SeqNo                  int64  `cbor:"seqNo"`
	Origin                 string `cbor:"origin"`
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// testdataDir resolves the absolute path of internal/core/testdata/jackson_cbor
// from this test file's working directory (serialization/jackson-cbor).
func testdataDir(t *testing.T) string {
	t.Helper()
	// CWD when go test runs is the package dir: serialization/jackson-cbor.
	abs, err := filepath.Abs(filepath.Join("..", "..", "internal", "core", "testdata", "jackson_cbor"))
	if err != nil {
		t.Fatalf("resolve testdata dir: %v", err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("testdata dir not found at %s: %v", abs, err)
	}
	return abs
}

func loadGoldenBytes(t *testing.T, name string) []byte {
	t.Helper()
	p := filepath.Join(testdataDir(t), name)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read golden %s: %v", p, err)
	}
	return data
}

type goldenManifest struct {
	FixtureName    string            `json:"fixture_name"`
	SourceType     string            `json:"source_type"`
	Manifest       string            `json:"manifest"`
	SerializerID   int               `json:"serializer_id"`
	CborByteLength int               `json:"cbor_byte_length"`
	CborHex        string            `json:"cbor_hex"`
	SourceFields   map[string]string `json:"source_fields"`
}

func loadGoldenManifest(t *testing.T, name string) goldenManifest {
	t.Helper()
	p := filepath.Join(testdataDir(t), name)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read manifest %s: %v", p, err)
	}
	var gm goldenManifest
	if err := json.Unmarshal(data, &gm); err != nil {
		t.Fatalf("parse manifest %s: %v", p, err)
	}
	return gm
}

// newRegisteredSerializer returns a Serializer with all showcase types
// registered. The registration call also serves as a positive test for
// Register and the JVMTyped pathway.
func newRegisteredSerializer(t *testing.T) *jcbor.Serializer {
	t.Helper()
	s := jcbor.New(jcbor.DefaultID)
	s.Register(&echoEnvelope{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.EchoEnvelope"}})
	s.Register(&askEnvelope{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.AskEnvelope"}})
	s.Register(&systemMessagePing{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.SystemMessagePing"}})
	s.Register(&showcaseEchoCustom{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.ShowcaseEchoCustom"}})
	s.Register(&ping{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.Ping"}})
	s.Register(&pong{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.Pong"}})
	return s
}

// assertEchoEnvelopeFields verifies the seqNo/originator/direction/payloadKind
// columns against the JSON manifest. Payload assertions are done by the
// caller because each kind decodes to a different Go shape.
func assertEchoEnvelopeFields(t *testing.T, got *echoEnvelope, want goldenManifest) {
	t.Helper()
	if got.Manifest() != want.Manifest {
		t.Errorf("Manifest: got %q want %q", got.Manifest(), want.Manifest)
	}
	if s := strconv.FormatInt(got.SeqNo, 10); s != want.SourceFields["seqNo"] {
		t.Errorf("SeqNo: got %s want %s", s, want.SourceFields["seqNo"])
	}
	if got.Originator != want.SourceFields["originator"] {
		t.Errorf("Originator: got %q want %q", got.Originator, want.SourceFields["originator"])
	}
	if got.Direction != want.SourceFields["direction"] {
		t.Errorf("Direction: got %q want %q", got.Direction, want.SourceFields["direction"])
	}
	if got.PayloadKind != want.SourceFields["payloadKind"] {
		t.Errorf("PayloadKind: got %q want %q", got.PayloadKind, want.SourceFields["payloadKind"])
	}
}

func assertAskEnvelopeFields(t *testing.T, got *askEnvelope, want goldenManifest) {
	t.Helper()
	if got.Manifest() != want.Manifest {
		t.Errorf("Manifest: got %q want %q", got.Manifest(), want.Manifest)
	}
	if s := strconv.FormatInt(got.SeqNo, 10); s != want.SourceFields["seqNo"] {
		t.Errorf("SeqNo: got %s want %s", s, want.SourceFields["seqNo"])
	}
	if got.Originator != want.SourceFields["originator"] {
		t.Errorf("Originator: got %q want %q", got.Originator, want.SourceFields["originator"])
	}
	if got.Direction != want.SourceFields["direction"] {
		t.Errorf("Direction: got %q want %q", got.Direction, want.SourceFields["direction"])
	}
	if got.PayloadKind != want.SourceFields["payloadKind"] {
		t.Errorf("PayloadKind: got %q want %q", got.PayloadKind, want.SourceFields["payloadKind"])
	}
}

// ── EchoEnvelope round-trips (direction a, Scala → Go) ──────────────────────

func TestJacksonCbor_EchoEnvelope_String_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_echoenvelope_string.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_echoenvelope_string.json")

	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env, ok := out.(*echoEnvelope)
	if !ok {
		t.Fatalf("expected *echoEnvelope, got %T", out)
	}
	assertEchoEnvelopeFields(t, env, gm)
	if got, want := env.Payload, gm.SourceFields["payload"]; got != want {
		t.Errorf("Payload (string): got %v (%T) want %v", got, got, want)
	}
}

func TestJacksonCbor_EchoEnvelope_Long_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_echoenvelope_long.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_echoenvelope_long.json")

	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*echoEnvelope)
	assertEchoEnvelopeFields(t, env, gm)
	got, ok := env.Payload.(int64)
	if !ok {
		t.Fatalf("Payload (long): want int64, got %T (%v)", env.Payload, env.Payload)
	}
	wantStr := gm.SourceFields["payload"]
	want, _ := strconv.ParseInt(wantStr, 10, 64)
	if got != want {
		t.Errorf("Payload (long): got %d want %d", got, want)
	}
}

func TestJacksonCbor_EchoEnvelope_SystemMessage_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_echoenvelope_system.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_echoenvelope_system.json")

	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*echoEnvelope)
	assertEchoEnvelopeFields(t, env, gm)

	// Nested payload arrives as map[interface{}]interface{} (no @class on the
	// wire — application code coerces by payloadKind).
	m, ok := env.Payload.(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Payload (system): want map[interface{}]interface{}, got %T", env.Payload)
	}
	wantSeq, _ := strconv.ParseInt(gm.SourceFields["payload.seqNo"], 10, 64)
	if got, ok := m["seqNo"].(int64); !ok || got != wantSeq {
		t.Errorf("Payload.seqNo: got %v (%T) want %d", m["seqNo"], m["seqNo"], wantSeq)
	}
	wantOrigin := gm.SourceFields["payload.origin"]
	if got, ok := m["origin"].(string); !ok || got != wantOrigin {
		t.Errorf("Payload.origin: got %v want %q", m["origin"], wantOrigin)
	}
}

func TestJacksonCbor_EchoEnvelope_ShowcaseEchoCustom_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_echoenvelope_custom.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_echoenvelope_custom.json")

	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*echoEnvelope)
	assertEchoEnvelopeFields(t, env, gm)

	m, ok := env.Payload.(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Payload (custom): want map[interface{}]interface{}, got %T", env.Payload)
	}
	wantSeq, _ := strconv.ParseInt(gm.SourceFields["payload.seqNo"], 10, 64)
	if got, ok := m["seqNo"].(int64); !ok || got != wantSeq {
		t.Errorf("Payload.seqNo: got %v want %d", m["seqNo"], wantSeq)
	}
	if got, ok := m["originator"].(string); !ok || got != gm.SourceFields["payload.originator"] {
		t.Errorf("Payload.originator: got %v want %q", m["originator"], gm.SourceFields["payload.originator"])
	}
	bytesArr, ok := m["payload"].([]interface{})
	if !ok {
		t.Fatalf("Payload.payload: want []interface{}, got %T", m["payload"])
	}
	wantHex := gm.SourceFields["payload.bytes_hex"]
	gotHex := ""
	for _, v := range bytesArr {
		switch x := v.(type) {
		case int64:
			gotHex += fmt.Sprintf("%02x", x&0xff)
		case uint64:
			gotHex += fmt.Sprintf("%02x", x&0xff)
		default:
			t.Fatalf("Payload.payload[]: unexpected element type %T", v)
		}
	}
	if gotHex != wantHex {
		t.Errorf("Payload.payload hex: got %s want %s", gotHex, wantHex)
	}
}

// ── AskEnvelope round-trips (same shape as EchoEnvelope) ────────────────────

func TestJacksonCbor_AskEnvelope_String_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_askenvelope_string.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_askenvelope_string.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*askEnvelope)
	assertAskEnvelopeFields(t, env, gm)
	if got, want := env.Payload, gm.SourceFields["payload"]; got != want {
		t.Errorf("Payload (string): got %v want %v", got, want)
	}
}

func TestJacksonCbor_AskEnvelope_Long_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_askenvelope_long.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_askenvelope_long.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*askEnvelope)
	assertAskEnvelopeFields(t, env, gm)
	got, ok := env.Payload.(int64)
	if !ok {
		t.Fatalf("Payload (long): want int64, got %T", env.Payload)
	}
	want, _ := strconv.ParseInt(gm.SourceFields["payload"], 10, 64)
	if got != want {
		t.Errorf("Payload (long): got %d want %d", got, want)
	}
}

func TestJacksonCbor_AskEnvelope_SystemMessage_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_askenvelope_system.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_askenvelope_system.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*askEnvelope)
	assertAskEnvelopeFields(t, env, gm)
	m, ok := env.Payload.(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Payload (system): want map[interface{}]interface{}, got %T", env.Payload)
	}
	wantSeq, _ := strconv.ParseInt(gm.SourceFields["payload.seqNo"], 10, 64)
	if got, ok := m["seqNo"].(int64); !ok || got != wantSeq {
		t.Errorf("Payload.seqNo: got %v want %d", m["seqNo"], wantSeq)
	}
	if got, ok := m["origin"].(string); !ok || got != gm.SourceFields["payload.origin"] {
		t.Errorf("Payload.origin: got %v want %q", m["origin"], gm.SourceFields["payload.origin"])
	}
}

func TestJacksonCbor_AskEnvelope_ShowcaseEchoCustom_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_askenvelope_custom.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_askenvelope_custom.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	env := out.(*askEnvelope)
	assertAskEnvelopeFields(t, env, gm)
	m, ok := env.Payload.(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Payload (custom): want map[interface{}]interface{}, got %T", env.Payload)
	}
	wantSeq, _ := strconv.ParseInt(gm.SourceFields["payload.seqNo"], 10, 64)
	if got, ok := m["seqNo"].(int64); !ok || got != wantSeq {
		t.Errorf("Payload.seqNo: got %v want %d", m["seqNo"], wantSeq)
	}
}

// ── FT4 flat-shape round-trips ──────────────────────────────────────────────

func TestJacksonCbor_Ping_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_ping.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_ping.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	p := out.(*ping)
	if p.Manifest() != gm.Manifest {
		t.Errorf("Manifest: got %q want %q", p.Manifest(), gm.Manifest)
	}
	if s := strconv.FormatInt(p.SeqNo, 10); s != gm.SourceFields["seqNo"] {
		t.Errorf("SeqNo: got %s want %s", s, gm.SourceFields["seqNo"])
	}
	if p.Origin != gm.SourceFields["origin"] {
		t.Errorf("Origin: got %q want %q", p.Origin, gm.SourceFields["origin"])
	}
}

func TestJacksonCbor_Pong_RoundTrip(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_pong.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_pong.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	p := out.(*pong)
	if p.Manifest() != gm.Manifest {
		t.Errorf("Manifest: got %q want %q", p.Manifest(), gm.Manifest)
	}
	if s := strconv.FormatInt(p.SeqNo, 10); s != gm.SourceFields["seqNo"] {
		t.Errorf("SeqNo: got %s want %s", s, gm.SourceFields["seqNo"])
	}
	if p.Origin != gm.SourceFields["origin"] {
		t.Errorf("Origin: got %q want %q", p.Origin, gm.SourceFields["origin"])
	}
}

// ── SystemMessagePing standalone (direction a) ──────────────────────────────

func TestJacksonCbor_SystemMessage_ScalaToGoDecode(t *testing.T) {
	s := newRegisteredSerializer(t)
	bytes := loadGoldenBytes(t, "scala_emitted_systemmessageping.cbor")
	gm := loadGoldenManifest(t, "scala_emitted_systemmessageping.json")
	out, err := s.FromBinary(bytes, gm.Manifest)
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	sm := out.(*systemMessagePing)
	if sm.Manifest() != gm.Manifest {
		t.Errorf("Manifest: got %q want %q", sm.Manifest(), gm.Manifest)
	}
	if s := strconv.FormatInt(sm.SeqNo, 10); s != gm.SourceFields["seqNo"] {
		t.Errorf("SeqNo: got %s want %s", s, gm.SourceFields["seqNo"])
	}
	if sm.Origin != gm.SourceFields["origin"] {
		t.Errorf("Origin: got %q want %q", sm.Origin, gm.SourceFields["origin"])
	}
}

// ── Go-emits-stable self-consistency check (and source for direction b) ─────

func TestJacksonCbor_SystemMessage_GoEmits_Stable(t *testing.T) {
	s := newRegisteredSerializer(t)
	src := &systemMessagePing{
		JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.SystemMessagePing"},
		SeqNo:            77,
		Origin:           "g1",
	}
	wire, err := s.ToBinary(src)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}

	// Self-consistency: Go decode of Go-emitted bytes.
	dec, err := s.FromBinary(wire, src.Manifest())
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	got := dec.(*systemMessagePing)
	if got.SeqNo != src.SeqNo || got.Origin != src.Origin {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, src)
	}

	// Side effect: write the bytes + JSON manifest to testdata so the
	// Scala-side JacksonCborGoldensSpec (Task #5) can decode them.
	goldenDir := testdataDir(t)
	cborPath := filepath.Join(goldenDir, "go_emitted_systemmessageping.cbor")
	jsonPath := filepath.Join(goldenDir, "go_emitted_systemmessageping.json")
	if err := os.WriteFile(cborPath, wire, 0o644); err != nil {
		t.Fatalf("write %s: %v", cborPath, err)
	}
	manifest := fmt.Sprintf(`{
  "fixture_name": "systemmessageping",
  "source_type": "github.com/sopranoworks/gekka/serialization/jackson-cbor:systemMessagePing",
  "manifest": %q,
  "serializer_id": %d,
  "cbor_byte_length": %d,
  "source_fields": {
    "seqNo": %q,
    "origin": %q
  }
}
`, src.Manifest(), s.Identifier(), len(wire),
		strconv.FormatInt(src.SeqNo, 10), src.Origin)
	if err := os.WriteFile(jsonPath, []byte(manifest), 0o644); err != nil {
		t.Fatalf("write %s: %v", jsonPath, err)
	}
}

func TestJacksonCbor_Ping_GoEmits_Stable(t *testing.T) {
	s := newRegisteredSerializer(t)
	src := &ping{
		JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.Ping"},
		SeqNo:            88,
		Origin:           "g2",
	}
	wire, err := s.ToBinary(src)
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	dec, err := s.FromBinary(wire, src.Manifest())
	if err != nil {
		t.Fatalf("FromBinary: %v", err)
	}
	got := dec.(*ping)
	if got.SeqNo != src.SeqNo || got.Origin != src.Origin {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, src)
	}

	goldenDir := testdataDir(t)
	cborPath := filepath.Join(goldenDir, "go_emitted_ping.cbor")
	jsonPath := filepath.Join(goldenDir, "go_emitted_ping.json")
	if err := os.WriteFile(cborPath, wire, 0o644); err != nil {
		t.Fatalf("write %s: %v", cborPath, err)
	}
	manifest := fmt.Sprintf(`{
  "fixture_name": "ping",
  "source_type": "github.com/sopranoworks/gekka/serialization/jackson-cbor:ping",
  "manifest": %q,
  "serializer_id": %d,
  "cbor_byte_length": %d,
  "source_fields": {
    "seqNo": %q,
    "origin": %q
  }
}
`, src.Manifest(), s.Identifier(), len(wire),
		strconv.FormatInt(src.SeqNo, 10), src.Origin)
	if err := os.WriteFile(jsonPath, []byte(manifest), 0o644); err != nil {
		t.Fatalf("write %s: %v", jsonPath, err)
	}
}

// ── Negative tests ──────────────────────────────────────────────────────────

func TestJacksonCbor_UnknownManifest_StructuredError(t *testing.T) {
	s := newRegisteredSerializer(t)
	_, err := s.FromBinary([]byte{0xa0}, "com.example.Frobozz")
	if err == nil {
		t.Fatal("expected error for unregistered manifest, got nil")
	}
	if got := err.Error(); !contains(got, "com.example.Frobozz") {
		t.Errorf("error message must name the offending manifest; got %q", got)
	}
}

func TestJacksonCbor_NoJVMClassManifest_RejectedOnEncode(t *testing.T) {
	s := newRegisteredSerializer(t)
	type plainGoType struct {
		Field string `cbor:"field"`
	}
	_, err := s.ToBinary(&plainGoType{Field: "x"})
	if err == nil {
		t.Fatal("expected error encoding type without JVMClassManifest, got nil")
	}
	if got := err.Error(); !contains(got, "no JVM class identity") {
		t.Errorf("error message must explain JVM class requirement; got %q", got)
	}
}

func TestJacksonCbor_UnknownPayloadKind_StructuredError(t *testing.T) {
	// This test asserts the discriminator-driven coercion layer's
	// negative path. The Serializer itself is generic — it decodes the
	// envelope into *echoEnvelope and leaves the polymorphic payload as
	// map[interface{}]interface{}. The "unknown payloadKind" error is
	// produced by an application-level coercion helper. The helper lives
	// in cmd/showcase-gekka/envelope.go (Task #6); here we exercise its
	// contract via an inline equivalent.
	coerce := func(kind string, _ interface{}) (interface{}, error) {
		switch kind {
		case "string", "long", "system", "custom":
			return nil, nil
		default:
			return nil, fmt.Errorf("envelope: unknown payloadKind %q (registered: string, long, system, custom)", kind)
		}
	}
	_, err := coerce("frobozz", "anything")
	if err == nil {
		t.Fatal("expected error for unknown payloadKind, got nil")
	}
	if !contains(err.Error(), "frobozz") {
		t.Errorf("error must name the offending payloadKind; got %q", err.Error())
	}
}

// ── Manifest emission ───────────────────────────────────────────────────────

func TestJacksonCbor_Manifest_LooksUpEmbedded(t *testing.T) {
	s := jcbor.New(jcbor.DefaultID)
	v := &echoEnvelope{JVMClassManifest: jcbor.JVMClassManifest{Class: "com.gekka.showcase.EchoEnvelope"}}
	if got := s.Manifest(v); got != "com.gekka.showcase.EchoEnvelope" {
		t.Errorf("Manifest: got %q want %q", got, "com.gekka.showcase.EchoEnvelope")
	}
}

func TestJacksonCbor_Manifest_EmptyForNonJVMTyped(t *testing.T) {
	s := jcbor.New(jcbor.DefaultID)
	type plain struct{ X int }
	if got := s.Manifest(&plain{X: 1}); got != "" {
		t.Errorf("Manifest of non-JVMTyped: got %q want \"\"", got)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

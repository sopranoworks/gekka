/*
 * jacksoncbor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package jacksoncbor implements Pekko's jackson-cbor wire format for the
// gekka transport layer. The wire shape is what Pekko's
// org.apache.pekko.serialization.jackson.JacksonCborSerializer produces for a
// Scala/Java case class — CBOR major-type-5 maps keyed by case-class field
// names, with no @class type tag for polymorphic AnyRef slots (verified
// empirically 2026-05-18 against scala_emitted_*.cbor goldens).
//
// # JVM class identity
//
// JVM class identity is carried by the Go type itself via an embeddable
// JVMClassManifest struct. Only Go types that embed JVMClassManifest are
// Pekko-wire-compatible; the serializer rejects encode attempts on
// non-JVM-typed values rather than auto-deriving a manifest from the Go
// type name. This avoids the haphazard "guess the JVM class from the Go
// reflection" anti-pattern.
//
//	type EchoEnvelope struct {
//	    jacksoncbor.JVMClassManifest `cbor:"-"`
//	    SeqNo      int64       `cbor:"seqNo"`
//	    Originator string      `cbor:"originator"`
//	    Direction  string      `cbor:"direction"`
//	    PayloadKind string     `cbor:"payloadKind"`
//	    Payload    interface{} `cbor:"payload"`
//	}
//
// Construction must set the embedded JVMClassManifest's Class field:
//
//	env := &EchoEnvelope{
//	    JVMClassManifest: jacksoncbor.JVMClassManifest{Class: "com.gekka.showcase.EchoEnvelope"},
//	    SeqNo: 1, ...
//	}
//
// Or use a constructor that sets it by convention; the rule is enforced at
// encode time — Register also asserts the prototype has a non-empty Class.
//
// # Polymorphic AnyRef slots
//
// Pekko's Jackson default does NOT emit @class for AnyRef fields nested
// inside a case class. The receiver-side type-coercion mechanism is
// application-level (the showcase uses the EchoEnvelope.PayloadKind
// discriminator). The serializer treats the AnyRef slot as `interface{}`
// and lets the CBOR decoder return whatever the wire shape implies (string,
// int64, []interface{} for byte arrays, map[interface{}]interface{} for
// nested case-class fields). Application code coerces from there based on
// the discriminator.
//
// # Serializer ID
//
// DefaultID = 33 matches Pekko's empirical assignment for jackson-cbor in
// the gekka_showcase_test cluster as of 2026-05-18. Real Pekko deployments
// announce the ID dynamically via CompressionTableAdvertisement frames; the
// per-association id-resolver in internal/core re-registers this Serializer
// instance under whatever ID an inbound peer announces, so DefaultID is
// only a bootstrap value.
package jacksoncbor

import (
	"fmt"
	"reflect"
	"sync"

	fxcbor "github.com/fxamacker/cbor/v2"
)

// DefaultID is the bootstrap serializer ID. Pekko's CompressionTableAdvertisement
// can override at runtime via the id-resolver in internal/core.
const DefaultID int32 = 33

// JVMClassManifest carries the JVM-side class identity for a Go type. Embed it
// (tagged `cbor:"-"` to keep it off the wire) into any Go type that has a
// JVM-side equivalent registered with the Pekko cluster.
type JVMClassManifest struct {
	// Class is the fully-qualified JVM class name, e.g.
	// "com.gekka.showcase.EchoEnvelope". Must be non-empty for the
	// serializer to encode the value.
	Class string
}

// Manifest returns the embedded JVM class name. Satisfies JVMTyped.
func (j JVMClassManifest) Manifest() string { return j.Class }

// JVMTyped is the contract for Pekko-wire compatibility. Types embedding
// JVMClassManifest satisfy it automatically.
type JVMTyped interface {
	Manifest() string
}

// ArterySerializerID makes JVMClassManifest satisfy the
// actor.RemoteSerializable interface (defined in actor/router.go). The
// outbound `prepareMessage` path checks this interface BEFORE falling back
// to JSON; without it, *EchoEnvelope etc. would route to JSONSerializer
// (sid=9) with manifest = Go reflect-type-name, completely bypassing the
// JacksonCborSerializer that core.SerializationRegistry knows about.
//
// Pekko's jackson-cbor identifier is class-defined as 33 (see
// pekko/serialization-jackson/src/main/resources/reference.conf:274 and
// the package doc above).
func (j JVMClassManifest) ArterySerializerID() int32 { return DefaultID }

// ArteryManifest returns the JVM class string as the wire manifest. Same
// embed; same string; satisfies actor.RemoteSerializable alongside
// ArterySerializerID.
func (j JVMClassManifest) ArteryManifest() string { return j.Class }

// Serializer implements the gekka transport serializer interface for
// Pekko's jackson-cbor wire format. Safe for concurrent use after
// registration.
type Serializer struct {
	id int32

	mu              sync.RWMutex
	typesByManifest map[string]reflect.Type // manifest -> non-pointer Go type
	manifestByType  map[reflect.Type]string // *T -> manifest (T is the element type)

	encMode fxcbor.EncMode
	decMode fxcbor.DecMode
}

// New constructs a Serializer at the given ID. Use DefaultID at bootstrap;
// the id-resolver may register the same instance under an additional ID
// once the cluster's CompressionTableAdvertisement arrives.
func New(id int32) *Serializer {
	em, _ := fxcbor.EncOptions{}.EncMode()
	dm, _ := fxcbor.DecOptions{
		// IntDec=IntDecConvertSigned so CBOR positive ints decode into int64
		// when target is interface{} (matches Pekko's java.lang.Long shape).
		IntDec: fxcbor.IntDecConvertSigned,
	}.DecMode()
	return &Serializer{
		id:              id,
		typesByManifest: make(map[string]reflect.Type),
		manifestByType:  make(map[reflect.Type]string),
		encMode:         em,
		decMode:         dm,
	}
}

// Identifier returns the serializer ID.
func (s *Serializer) Identifier() int32 { return s.id }

// Register adds a prototype to the type registry. The prototype must be a
// pointer (or a value) whose underlying type embeds JVMClassManifest and has a
// non-empty Class string. Panics on misuse — this is a startup-time call
// and silent misregistration would surface as obscure encode failures at
// runtime.
func (s *Serializer) Register(prototype JVMTyped) {
	manifest := prototype.Manifest()
	if manifest == "" {
		panic(fmt.Sprintf("jacksoncbor: Register called with empty Manifest() for %T", prototype))
	}
	elem := reflect.TypeOf(prototype)
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	ptrType := reflect.PointerTo(elem)

	s.mu.Lock()
	if existing, ok := s.typesByManifest[manifest]; ok && existing != elem {
		s.mu.Unlock()
		panic(fmt.Sprintf("jacksoncbor: manifest %q already registered to %v, cannot re-register to %v",
			manifest, existing, elem))
	}
	s.typesByManifest[manifest] = elem
	s.manifestByType[ptrType] = manifest
	s.mu.Unlock()
}

// Manifest returns the JVM class manifest for msg, or "" if msg does not
// satisfy JVMTyped. The lookup walks: value-receiver JVMTyped → pointer
// dereference → manifestByType reverse map.
func (s *Serializer) Manifest(msg any) string {
	if jt, ok := msg.(JVMTyped); ok {
		if c := jt.Manifest(); c != "" {
			return c
		}
	}
	rv := reflect.ValueOf(msg)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return ""
		}
		if jt, ok := rv.Elem().Interface().(JVMTyped); ok {
			if c := jt.Manifest(); c != "" {
				return c
			}
		}
		s.mu.RLock()
		m := s.manifestByType[rv.Type()]
		s.mu.RUnlock()
		return m
	}
	return ""
}

// ToBinary encodes msg to CBOR bytes. Errors if msg does not have a JVM
// class identity (either via JVMTyped or via Register lookup) — the
// serializer never auto-derives a manifest from the Go type name. The
// caller's responsibility is to ensure msg embeds JVMClassManifest with a
// non-empty Class.
func (s *Serializer) ToBinary(msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("jacksoncbor: ToBinary called with nil msg")
	}
	if s.Manifest(msg) == "" {
		return nil, fmt.Errorf("jacksoncbor: %T has no JVM class identity (embed JVMClassManifest with non-empty Class, or register a prototype)", msg)
	}
	return s.encMode.Marshal(msg)
}

// FromBinary decodes CBOR bytes into the Go type registered for manifest.
// Returns a pointer to a freshly-allocated instance of that type. If the
// manifest is not registered, returns a structured error that includes the
// manifest string so the inbound dispatcher logs a debuggable diagnostic
// rather than crashing.
func (s *Serializer) FromBinary(data []byte, manifest string) (interface{}, error) {
	if manifest == "" {
		return nil, fmt.Errorf("jacksoncbor: FromBinary called with empty manifest")
	}
	s.mu.RLock()
	elem, ok := s.typesByManifest[manifest]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("jacksoncbor: no Go type registered for JVM manifest %q (register a prototype with JVMClassManifest{Class: %q})", manifest, manifest)
	}
	ptr := reflect.New(elem)
	if err := s.decMode.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("jacksoncbor: decode manifest %q: %w", manifest, err)
	}
	// Stamp the JVMClassManifest.Class so the decoded value self-describes.
	if jt, ok := ptr.Interface().(JVMTyped); ok && jt.Manifest() == "" {
		stampManifest(ptr.Elem(), manifest)
	}
	return ptr.Interface(), nil
}

// stampManifest finds the embedded JVMClassManifest field in v (a struct value)
// and sets its Class field. No-op if not found.
func stampManifest(v reflect.Value, manifest string) {
	if v.Kind() != reflect.Struct {
		return
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
			continue
		}
		if f.Type == reflect.TypeOf(JVMClassManifest{}) {
			classField := v.Field(i).FieldByName("Class")
			if classField.IsValid() && classField.CanSet() {
				classField.SetString(manifest)
			}
			return
		}
	}
}

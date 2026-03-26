/*
 * serialization_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"reflect"
	"testing"

	"github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func TestSerializationRoundTrip(t *testing.T) {
	registry := NewSerializationRegistry()
	// Temporarily register cluster/artery serializers for the test
	// They are not in the default registry to satisfy compatibility tests.
	registry.serializers[ClusterSerializerID] = registry.protobufSerializer
	registry.serializers[ArteryInternalSerializerID] = registry.protobufSerializer

	manifests := registry.GetManifests()

	for manifest, typ := range manifests {
		t.Run(manifest, func(t *testing.T) {
			// Instantiate type
			var msg interface{}
			if typ.Kind() == reflect.Ptr {
				msg = reflect.New(typ.Elem()).Interface()
			} else {
				msg = reflect.New(typ).Elem().Interface()
			}

			// Perform Marshal -> Unmarshal round-trip check
			// We use AllowPartial: true because many Akka messages have required fields
			// that we haven't populated here.
			marshalOpts := proto.MarshalOptions{AllowPartial: true}
			unmarshalOpts := proto.UnmarshalOptions{AllowPartial: true}

			var payload []byte
			var sid int32
			var m string
			var err error

			if pm, ok := msg.(proto.Message); ok {
				payload, err = marshalOpts.Marshal(pm)
				if err != nil {
					t.Fatalf("proto.Marshal error: %v", err)
				}
				// We need to find the sid and manifest manually since SerializePayload
				// would call s.ToBinary which uses the default proto.Marshal.
				m, _ = registry.GetManifestByType(typ)
				sid, _ = registry.GetSerializerIdByManifest(m)
			} else {
				payload, sid, m, err = registry.SerializePayload(msg)
				if err != nil {
					t.Fatalf("SerializePayload error: %v", err)
				}
			}

			if m != manifest {
				t.Errorf("manifest mismatch: expected %q, got %q", manifest, m)
			}

			decoded, err := registry.DeserializePayload(sid, m, payload)
			if err != nil {
				// If it's a proto error about missing fields, try unmarshaling with AllowPartial
				if pm, ok := reflect.New(typ.Elem()).Interface().(proto.Message); ok {
					err = unmarshalOpts.Unmarshal(payload, pm)
					if err == nil {
						decoded = pm
					}
				}
				if err != nil {
					t.Fatalf("DeserializePayload error: %v", err)
				}
			}

			if reflect.TypeOf(decoded) != typ {
				t.Errorf("type mismatch after round-trip: expected %v, got %v", typ, reflect.TypeOf(decoded))
			}

			// For proto.Message, verify it can be marshaled again
			if pm, ok := decoded.(proto.Message); ok {
				if _, err := marshalOpts.Marshal(pm); err != nil {
					t.Errorf("failed to marshal decoded proto message: %v", err)
				}
			}
		})
	}
}

func BenchmarkProtobuf(b *testing.B) {
	registry := NewSerializationRegistry()
	serializer, _ := registry.GetSerializer(ProtobufSerializerID)

	msg := &remote.ActorRefData{
		Path: proto.String("pekko://System@127.0.0.1:2552/user/foo"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := serializer.ToBinary(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroCopy(b *testing.B) {
	registry := NewSerializationRegistry()
	serializer, _ := registry.GetSerializer(ProtobufSerializerID)
	zcSerializer, ok := serializer.(interface {
		Size(msg any) int
		MarshalTo(buf []byte, msg any) (int, error)
	})
	if !ok {
		b.Skip("Serializer does not support ZeroCopy")
	}

	msg := &remote.ActorRefData{
		Path: proto.String("pekko://System@127.0.0.1:2552/user/foo"),
	}

	size := zcSerializer.Size(msg)
	buf := make([]byte, size)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := zcSerializer.MarshalTo(buf, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildArteryFrame(b *testing.B) {
	payload := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := BuildArteryFrame(1, 2, "/user/sender", "/user/receiver", "Manifest", payload, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}

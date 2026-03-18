/*
 * serialization_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"testing"

	gproto_remote "github.com/sopranoworks/gekka/internal/proto/remote"
	"google.golang.org/protobuf/proto"
)

func BenchmarkProtobuf(b *testing.B) {
	registry := NewSerializationRegistry()
	serializer, _ := registry.GetSerializer(ProtobufSerializerID)
	
	msg := &gproto_remote.ActorRefData{
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
	zcSerializer := serializer.(ZeroCopySerializer)
	
	msg := &gproto_remote.ActorRefData{
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

func BenchmarkArteryFrameBuilder(b *testing.B) {
	builder := NewArteryFrameBuilder()
	payload := make([]byte, 1024)
	
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := builder.BuildToBuffers(1, 2, "/user/sender", "/user/receiver", "Manifest", payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

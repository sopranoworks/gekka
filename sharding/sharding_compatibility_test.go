/*
 * sharding/sharding_compatibility_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sopranoworks/gekka/sharding"
)

// loadFixture reads a binary fixture from testdata/.
// If the file does not exist the test is skipped with regeneration instructions.
func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("fixture %s not found — regenerate with:\n"+
			"  cd scala-server && sbt \"runMain org.apache.pekko.cluster.sharding.ShardingBinaryGenerator ../sharding/testdata\"\n"+
			"  go test ./sharding/ -v -count=1", name)
	}
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// ---------------------------------------------------------------------------
// Serializer ID and manifest constants
// ---------------------------------------------------------------------------

func TestShardingSerializerID(t *testing.T) {
	if sharding.ShardingSerializerID != 13 {
		t.Errorf("ShardingSerializerID = %d, want 13", sharding.ShardingSerializerID)
	}
}

func TestShardingManifests(t *testing.T) {
	cases := []struct{ name, got, want string }{
		{"CoordinatorState", sharding.CoordinatorStateManifest, "AA"},
		{"ShardHomeAllocated", sharding.ShardHomeAllocatedManifest, "AF"},
		{"ShardHomeDeallocated", sharding.ShardHomeDeallocatedManifest, "AG"},
		{"Register", sharding.RegisterManifest, "BA"},
		{"RegisterAck", sharding.RegisterAckManifest, "BC"},
		{"GetShardHome", sharding.GetShardHomeManifest, "BD"},
		{"ShardHome", sharding.ShardHomeManifest, "BE"},
		{"HostShard", sharding.HostShardManifest, "BF"},
		{"ShardStarted", sharding.ShardStartedManifest, "BG"},
		{"BeginHandOff", sharding.BeginHandOffManifest, "BH"},
		{"BeginHandOffAck", sharding.BeginHandOffAckManifest, "BI"},
		{"HandOff", sharding.HandOffManifest, "BJ"},
		{"ShardStopped", sharding.ShardStoppedManifest, "BK"},
		{"StartEntity", sharding.StartEntityManifest, "EA"},
		{"StartEntityAck", sharding.StartEntityAckManifest, "EB"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s manifest = %q, want %q", c.name, c.got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// StartEntity (manifest "EA")
// Proto: StartEntity { required string entityId = 1 }
// Scala fixture: StartEntity("entity-1")
// Expected bytes (10): 0a 08 65 6e 74 69 74 79 2d 31
// ---------------------------------------------------------------------------

func TestStartEntity_ScalaDecode(t *testing.T) {
	data := loadFixture(t, "sharding_startentity.bin")
	ser := &sharding.ShardingSerializer{}
	entityId, err := ser.DecodeStartEntity(data)
	if err != nil {
		t.Fatalf("DecodeStartEntity: %v", err)
	}
	if entityId != "entity-1" {
		t.Errorf("entityId = %q, want %q", entityId, "entity-1")
	}
}

func TestStartEntity_GoMatchesScala(t *testing.T) {
	scala := loadFixture(t, "sharding_startentity.bin")
	ser := &sharding.ShardingSerializer{}
	got := ser.EncodeStartEntity("entity-1")
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes = %x\nScala     = %x", got, scala)
	}
}

func TestStartEntity_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	encoded := ser.EncodeStartEntity("my-entity")
	got, err := ser.DecodeStartEntity(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "my-entity" {
		t.Errorf("got %q, want %q", got, "my-entity")
	}
}

// ---------------------------------------------------------------------------
// StartEntityAck (manifest "EB")
// Proto: StartEntityAck { required string entityId = 1; required string shardId = 2 }
// Scala fixture: StartEntityAck("entity-1", "shard-42")
// Expected bytes (20): 0a 08 "entity-1" 12 08 "shard-42"
// ---------------------------------------------------------------------------

func TestStartEntityAck_ScalaDecode(t *testing.T) {
	data := loadFixture(t, "sharding_startentityack.bin")
	ser := &sharding.ShardingSerializer{}
	entityId, shardId, err := ser.DecodeStartEntityAck(data)
	if err != nil {
		t.Fatalf("DecodeStartEntityAck: %v", err)
	}
	if entityId != "entity-1" {
		t.Errorf("entityId = %q, want %q", entityId, "entity-1")
	}
	if shardId != "shard-42" {
		t.Errorf("shardId = %q, want %q", shardId, "shard-42")
	}
}

func TestStartEntityAck_GoMatchesScala(t *testing.T) {
	scala := loadFixture(t, "sharding_startentityack.bin")
	ser := &sharding.ShardingSerializer{}
	got := ser.EncodeStartEntityAck("entity-1", "shard-42")
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes = %x\nScala     = %x", got, scala)
	}
}

func TestStartEntityAck_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	encoded := ser.EncodeStartEntityAck("ent-99", "sh-1")
	eId, sId, err := ser.DecodeStartEntityAck(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if eId != "ent-99" || sId != "sh-1" {
		t.Errorf("got (%q,%q), want (%q,%q)", eId, sId, "ent-99", "sh-1")
	}
}

// ---------------------------------------------------------------------------
// GetShardHome (manifest "BD") — uses ShardIdMessage proto
// Proto: ShardIdMessage { required string shard = 1 }
// Scala fixture: GetShardHome("shard-7")
// Expected bytes (9): 0a 07 "shard-7"
// ---------------------------------------------------------------------------

func TestGetShardHome_ScalaDecode(t *testing.T) {
	data := loadFixture(t, "sharding_getshardhome.bin")
	ser := &sharding.ShardingSerializer{}
	shardId, err := ser.DecodeGetShardHome(data)
	if err != nil {
		t.Fatalf("DecodeGetShardHome: %v", err)
	}
	if shardId != "shard-7" {
		t.Errorf("shardId = %q, want %q", shardId, "shard-7")
	}
}

func TestGetShardHome_GoMatchesScala(t *testing.T) {
	scala := loadFixture(t, "sharding_getshardhome.bin")
	ser := &sharding.ShardingSerializer{}
	got := ser.EncodeGetShardHome("shard-7")
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes = %x\nScala     = %x", got, scala)
	}
}

func TestGetShardHome_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	encoded := ser.EncodeGetShardHome("shard-123")
	got, err := ser.DecodeGetShardHome(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "shard-123" {
		t.Errorf("got %q, want %q", got, "shard-123")
	}
}

// ---------------------------------------------------------------------------
// ShardHomeAllocated (manifest "AF")
// Proto: ShardHomeAllocated { required string shard = 1; required string region = 2 }
// Scala fixture: ShardHomeAllocated("shard-42", testRegion)
// region = Serialization.serializedActorPath(ref) — includes non-deterministic UID suffix
// Strategy: decode fixture to extract exact path, then re-encode and compare.
// ---------------------------------------------------------------------------

func TestShardHomeAllocated_ScalaDecode(t *testing.T) {
	data := loadFixture(t, "sharding_shardhomeallocated.bin")
	ser := &sharding.ShardingSerializer{}
	shardId, regionPath, err := ser.DecodeShardHomeAllocated(data)
	if err != nil {
		t.Fatalf("DecodeShardHomeAllocated: %v", err)
	}
	if shardId != "shard-42" {
		t.Errorf("shardId = %q, want %q", shardId, "shard-42")
	}
	// Region path includes a non-deterministic UID (e.g., "pekko://BinaryGen/user/testRegion#1234567890")
	// Just verify the deterministic prefix.
	want := "pekko://BinaryGen/user/testRegion"
	if len(regionPath) < len(want) || regionPath[:len(want)] != want {
		t.Errorf("regionPath = %q, want prefix %q", regionPath, want)
	}
	t.Logf("regionPath = %q", regionPath)
}

func TestShardHomeAllocated_GoMatchesScala(t *testing.T) {
	scala := loadFixture(t, "sharding_shardhomeallocated.bin")
	ser := &sharding.ShardingSerializer{}
	// Decode the Scala fixture first to extract the exact non-deterministic path.
	_, regionPath, err := ser.DecodeShardHomeAllocated(scala)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	// Re-encode using the decoded path — must produce byte-identical output.
	got := ser.EncodeShardHomeAllocated("shard-42", regionPath)
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes = %x\nScala     = %x", got, scala)
	}
}

func TestShardHomeAllocated_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	path := "pekko://MySystem@10.0.0.1:2552/user/shardRegion#987654321"
	encoded := ser.EncodeShardHomeAllocated("shard-5", path)
	sId, rPath, err := ser.DecodeShardHomeAllocated(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sId != "shard-5" || rPath != path {
		t.Errorf("got (%q,%q), want (%q,%q)", sId, rPath, "shard-5", path)
	}
}

// ---------------------------------------------------------------------------
// BeginHandOff (manifest "BH") — uses ShardIdMessage proto
// Proto: ShardIdMessage { required string shard = 1 }
// Scala fixture: BeginHandOff("shard-7")
// Expected bytes (9): 0a 07 "shard-7" — identical to GetShardHome fixture
// ---------------------------------------------------------------------------

func TestBeginHandOff_ScalaDecode(t *testing.T) {
	data := loadFixture(t, "sharding_beginhandoff.bin")
	ser := &sharding.ShardingSerializer{}
	shardId, err := ser.DecodeBeginHandOff(data)
	if err != nil {
		t.Fatalf("DecodeBeginHandOff: %v", err)
	}
	if shardId != "shard-7" {
		t.Errorf("shardId = %q, want %q", shardId, "shard-7")
	}
}

func TestBeginHandOff_GoMatchesScala(t *testing.T) {
	scala := loadFixture(t, "sharding_beginhandoff.bin")
	ser := &sharding.ShardingSerializer{}
	got := ser.EncodeBeginHandOff("shard-7")
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes = %x\nScala     = %x", got, scala)
	}
}

func TestBeginHandOff_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	encoded := ser.EncodeBeginHandOff("shard-99")
	got, err := ser.DecodeBeginHandOff(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "shard-99" {
		t.Errorf("got %q, want %q", got, "shard-99")
	}
}

// ---------------------------------------------------------------------------
// Cross-message assertions
// ---------------------------------------------------------------------------

// TestCompressionBoundary_AllPlainProto verifies that none of the 5 sharding
// fixtures are GZIP-compressed. Only CoordinatorState (manifest "AA") uses GZIP;
// all other messages are plain Protobuf.
func TestCompressionBoundary_AllPlainProto(t *testing.T) {
	fixtures := []string{
		"sharding_startentity.bin",
		"sharding_startentityack.bin",
		"sharding_getshardhome.bin",
		"sharding_shardhomeallocated.bin",
		"sharding_beginhandoff.bin",
	}
	gzipMagic := []byte{0x1f, 0x8b}
	for _, name := range fixtures {
		data := loadFixture(t, name)
		if len(data) >= 2 && bytes.Equal(data[:2], gzipMagic) {
			t.Errorf("%s has GZIP magic — expected plain Protobuf", name)
		}
	}
}

// TestGetShardHomeSameProtoAsBeginHandOff confirms that GetShardHome and
// BeginHandOff share the same ShardIdMessage proto schema and produce
// byte-identical output for the same shard ID.
func TestGetShardHomeSameProtoAsBeginHandOff(t *testing.T) {
	gsh := loadFixture(t, "sharding_getshardhome.bin")
	bho := loadFixture(t, "sharding_beginhandoff.bin")
	if !bytes.Equal(gsh, bho) {
		t.Errorf("GetShardHome and BeginHandOff fixtures differ for same shard-7:\n"+
			"GetShardHome = %x\nBeginHandOff = %x", gsh, bho)
	}
	// Also verify the Go encoders produce the same bytes.
	ser := &sharding.ShardingSerializer{}
	gshGo := ser.EncodeGetShardHome("shard-7")
	bhoGo := ser.EncodeBeginHandOff("shard-7")
	if !bytes.Equal(gshGo, bhoGo) {
		t.Errorf("Go encoders differ:\nEncodeGetShardHome = %x\nEncodeBeginHandOff = %x", gshGo, bhoGo)
	}
}

// ---------------------------------------------------------------------------
// ShardHome (manifest "BE") — same proto schema as ShardHomeAllocated
// No Scala fixture; test Go round-trip only.
// ---------------------------------------------------------------------------

func TestShardHome_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	path := "pekko://Sys@192.168.1.1:2552/user/region#42"
	encoded := ser.EncodeShardHome("s-1", path)
	sId, rPath, err := ser.DecodeShardHome(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sId != "s-1" || rPath != path {
		t.Errorf("got (%q,%q), want (%q,%q)", sId, rPath, "s-1", path)
	}
}

// ---------------------------------------------------------------------------
// ActorRefMessage — shared proto for Register, RegisterAck, etc.
// No Scala fixture; test Go round-trip only.
// ---------------------------------------------------------------------------

func TestActorRefMessage_RoundTrip(t *testing.T) {
	ser := &sharding.ShardingSerializer{}
	actorPath := "pekko://Sys@10.0.0.1:2552/user/shardRegion#12345"
	encoded := ser.EncodeActorRefMessage(actorPath)
	got, err := ser.DecodeActorRefMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != actorPath {
		t.Errorf("got %q, want %q", got, actorPath)
	}
}

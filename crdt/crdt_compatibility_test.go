/*
 * crdt_compatibility_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package crdt_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sopranoworks/gekka/crdt"
)

// loadFixtureCRDT reads a binary fixture from testdata/.
// If the file does not exist the test is skipped with regeneration instructions.
func loadFixtureCRDT(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("fixture %s not found — regenerate with:\n"+
			"  cd scala-server && sbt \"runMain org.apache.pekko.cluster.ddata.DDataBinaryGenerator ../crdt/testdata\"\n"+
			"  go test ./crdt/ -v -count=1", name)
	}
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func decompressGzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll(gzip): %v", err)
	}
	return out
}

// ---------------------------------------------------------------------------
// Serializer ID and manifest constant tests
// ---------------------------------------------------------------------------

func TestDDataSerializerIDs(t *testing.T) {
	if crdt.DDataReplicatedSerializerID != 11 {
		t.Errorf("DDataReplicatedSerializerID = %d, want 11", crdt.DDataReplicatedSerializerID)
	}
	if crdt.DDataReplicatorMsgSerializerID != 12 {
		t.Errorf("DDataReplicatorMsgSerializerID = %d, want 12", crdt.DDataReplicatorMsgSerializerID)
	}
}

func TestDDataManifests(t *testing.T) {
	cases := []struct{ name, got, want string }{
		{"GCounter", crdt.GCounterManifest, "F"},
		{"ORSet", crdt.ORSetManifest, "C"},
		{"DeltaPropagation", crdt.DeltaPropagationManifest, "Q"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s manifest = %q, want %q", c.name, c.got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// GCounter tests
// ---------------------------------------------------------------------------

// TestGCounter_ScalaDecode decodes the Scala-generated GCounter fixture and
// verifies the two entries: node1@port2551/uid11111=42, node2@port2552/uid22222=7.
func TestGCounter_ScalaDecode(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_gcounter.bin")
	ser := &crdt.DDataSerializer{}
	entries, err := ser.DecodeGCounter(data)
	if err != nil {
		t.Fatalf("DecodeGCounter: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Node.Address.Port < entries[j].Node.Address.Port
	})

	e1 := entries[0]
	if e1.Node.Address.Hostname != "127.0.0.1" {
		t.Errorf("entry[0] hostname = %q, want %q", e1.Node.Address.Hostname, "127.0.0.1")
	}
	if e1.Node.Address.Port != 2551 {
		t.Errorf("entry[0] port = %d, want 2551", e1.Node.Address.Port)
	}
	if e1.Node.UID() != 11111 {
		t.Errorf("entry[0] uid = %d, want 11111", e1.Node.UID())
	}
	if e1.Value != 42 {
		t.Errorf("entry[0] value = %d, want 42", e1.Value)
	}

	e2 := entries[1]
	if e2.Node.Address.Port != 2552 {
		t.Errorf("entry[1] port = %d, want 2552", e2.Node.Address.Port)
	}
	if e2.Node.UID() != 22222 {
		t.Errorf("entry[1] uid = %d, want 22222", e2.Node.UID())
	}
	if e2.Value != 7 {
		t.Errorf("entry[1] value = %d, want 7", e2.Value)
	}
}

// TestGCounter_GoMatchesScala re-encodes the decoded GCounter entries and
// verifies byte-identical output to the Scala fixture.
func TestGCounter_GoMatchesScala(t *testing.T) {
	scala := loadFixtureCRDT(t, "ddata_gcounter.bin")
	ser := &crdt.DDataSerializer{}
	entries, err := ser.DecodeGCounter(scala)
	if err != nil {
		t.Fatalf("DecodeGCounter: %v", err)
	}
	got := ser.EncodeGCounter(entries)
	if !bytes.Equal(got, scala) {
		t.Errorf("Go bytes:\n  %x\nScala:  \n  %x", got, scala)
	}
}

func TestGCounter_RoundTrip(t *testing.T) {
	ser := &crdt.DDataSerializer{}
	entries := []crdt.DDGCounterEntry{
		{
			Node:  crdt.DDUniqueAddress{Address: crdt.DDAddress{Hostname: "10.0.0.1", Port: 2551}, UIDLow: 99},
			Value: 100,
		},
		{
			Node:  crdt.DDUniqueAddress{Address: crdt.DDAddress{Hostname: "10.0.0.2", Port: 2552}, UIDLow: 200},
			Value: 50,
		},
	}
	encoded := ser.EncodeGCounter(entries)
	decoded, err := ser.DecodeGCounter(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("got %d entries, want 2", len(decoded))
	}
	vals := map[uint32]uint64{}
	for _, e := range decoded {
		vals[e.Node.Address.Port] = e.Value
	}
	if vals[2551] != 100 {
		t.Errorf("port 2551 value = %d, want 100", vals[2551])
	}
	if vals[2552] != 50 {
		t.Errorf("port 2552 value = %d, want 50", vals[2552])
	}
}

// TestGCounter_BigIntEncoding verifies value 42 encodes as bytes 0x12 0x01 0x2a.
func TestGCounter_BigIntEncoding(t *testing.T) {
	ser := &crdt.DDataSerializer{}
	entries := []crdt.DDGCounterEntry{
		{Node: crdt.DDUniqueAddress{Address: crdt.DDAddress{Hostname: "127.0.0.1", Port: 2551}, UIDLow: 11111}, Value: 42},
	}
	got := ser.EncodeGCounter(entries)
	// Field 2 (value bytes), length=1, value=42 → 0x12 0x01 0x2a
	want := []byte{0x12, 0x01, 0x2a}
	if !bytes.Contains(got, want) {
		t.Errorf("expected value bytes %x not found in %x", want, got)
	}
}

// TestGCounter_NotGzip verifies the GCounter fixture is NOT GZIP-compressed.
func TestGCounter_NotGzip(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_gcounter.bin")
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		t.Error("GCounter fixture starts with GZIP magic — expected plain Protobuf")
	}
}

// ---------------------------------------------------------------------------
// ORSet tests
// ---------------------------------------------------------------------------

// TestORSet_IsGzipCompressed verifies the ORSet fixture starts with GZIP magic.
func TestORSet_IsGzipCompressed(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_orset.bin")
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		prefix := data
		if len(prefix) > 4 {
			prefix = prefix[:4]
		}
		t.Errorf("ORSet fixture does not start with GZIP magic: %x", prefix)
	}
}

// TestORSet_ScalaDecode decompresses and decodes the Scala ORSet fixture.
func TestORSet_ScalaDecode(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_orset.bin")
	ser := &crdt.DDataSerializer{}
	orset, err := ser.DecodeORSet(data)
	if err != nil {
		t.Fatalf("DecodeORSet: %v", err)
	}

	elems := make(map[string]bool)
	for _, e := range orset.Elements {
		elems[e] = true
	}
	if !elems["hello"] {
		t.Errorf("element %q not found; got %v", "hello", orset.Elements)
	}
	if !elems["world"] {
		t.Errorf("element %q not found; got %v", "world", orset.Elements)
	}
	// VVector: one sub-message containing 2 DotEntries (one per node)
	if len(orset.VVector) != 2 {
		t.Errorf("version vector entries = %d, want 2", len(orset.VVector))
	}
	// DotVectors: 2 DotVector blobs (one per element), each with 1 DotEntry
	if len(orset.DotVectors) != 2 {
		t.Errorf("dot vectors = %d, want 2", len(orset.DotVectors))
	}
	t.Logf("ORSet elements: %v, VVector: %d, DotVectors: %d",
		orset.Elements, len(orset.VVector), len(orset.DotVectors))
}

// TestORSet_GoMatchesScalaInner compares inner (decompressed) proto bytes.
// GZIP output is non-deterministic between JVM and Go Deflate engines,
// so we compare the decompressed inner bytes instead.
func TestORSet_GoMatchesScalaInner(t *testing.T) {
	scala := loadFixtureCRDT(t, "ddata_orset.bin")
	scalaInner := decompressGzipBytes(t, scala)

	ser := &crdt.DDataSerializer{}
	orset, err := ser.DecodeORSet(scala)
	if err != nil {
		t.Fatalf("DecodeORSet: %v", err)
	}
	goCompressed, err := ser.EncodeORSet(orset)
	if err != nil {
		t.Fatalf("EncodeORSet: %v", err)
	}
	goInner := decompressGzipBytes(t, goCompressed)

	if !bytes.Equal(goInner, scalaInner) {
		t.Errorf("inner proto mismatch:\n  Go:    %x\n  Scala: %x", goInner, scalaInner)
	}
}

func TestORSet_RoundTrip(t *testing.T) {
	ser := &crdt.DDataSerializer{}
	node1 := crdt.DDUniqueAddress{Address: crdt.DDAddress{Hostname: "10.0.0.1", Port: 2551}, UIDLow: 1}
	node2 := crdt.DDUniqueAddress{Address: crdt.DDAddress{Hostname: "10.0.0.2", Port: 2552}, UIDLow: 2}
	orset := &crdt.DDORSet{
		VVector:    []crdt.DDORSetDotEntry{{Node: node1, Counter: 1}, {Node: node2, Counter: 1}},
		DotVectors: [][]crdt.DDORSetDotEntry{{{Node: node1, Counter: 1}}, {{Node: node2, Counter: 1}}},
		Elements:   []string{"alpha", "beta"},
	}
	compressed, err := ser.EncodeORSet(orset)
	if err != nil {
		t.Fatalf("EncodeORSet: %v", err)
	}
	decoded, err := ser.DecodeORSet(compressed)
	if err != nil {
		t.Fatalf("DecodeORSet: %v", err)
	}
	elems := make(map[string]bool)
	for _, e := range decoded.Elements {
		elems[e] = true
	}
	if !elems["alpha"] || !elems["beta"] {
		t.Errorf("elements mismatch: got %v", decoded.Elements)
	}
}

// ---------------------------------------------------------------------------
// DeltaPropagation tests
// ---------------------------------------------------------------------------

// TestDeltaPropagation_ScalaDecode decodes the Scala DeltaPropagation fixture.
func TestDeltaPropagation_ScalaDecode(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_deltapropagation.bin")
	ser := &crdt.DDataSerializer{}
	dp, err := ser.DecodeDeltaPropagation(data)
	if err != nil {
		t.Fatalf("DecodeDeltaPropagation: %v", err)
	}

	if dp.FromNode.Address.Hostname != "127.0.0.1" {
		t.Errorf("fromNode hostname = %q, want %q", dp.FromNode.Address.Hostname, "127.0.0.1")
	}
	if dp.FromNode.Address.Port != 2551 {
		t.Errorf("fromNode port = %d, want 2551", dp.FromNode.Address.Port)
	}
	if dp.FromNode.UID() != 11111 {
		t.Errorf("fromNode uid = %d, want 11111", dp.FromNode.UID())
	}

	if len(dp.Deltas) != 1 {
		t.Fatalf("got %d delta entries, want 1", len(dp.Deltas))
	}
	de := dp.Deltas[0]
	if de.Key != "myCounter" {
		t.Errorf("key = %q, want %q", de.Key, "myCounter")
	}
	if de.SerializerID != 11 {
		t.Errorf("serializerID = %d, want 11", de.SerializerID)
	}
	if de.Manifest != "F" {
		t.Errorf("manifest = %q, want %q", de.Manifest, "F")
	}

	// Embedded data should decode as a GCounter
	gcEntries, err := ser.DecodeGCounter(de.Data)
	if err != nil {
		t.Fatalf("decode embedded GCounter: %v", err)
	}
	if len(gcEntries) == 0 {
		t.Error("embedded GCounter has no entries")
	}
	t.Logf("DeltaProp key=%q serializerId=%d manifest=%q innerGC entries=%d",
		de.Key, de.SerializerID, de.Manifest, len(gcEntries))
}

// TestDeltaPropagation_NotGzip verifies DeltaPropagation is NOT GZIP-compressed.
func TestDeltaPropagation_NotGzip(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_deltapropagation.bin")
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		t.Error("DeltaPropagation fixture is GZIP — expected plain Protobuf")
	}
}

// ---------------------------------------------------------------------------
// UniqueAddress encoding tests
// ---------------------------------------------------------------------------

// TestUniqueAddress_UIDSplit verifies UID is correctly split into low/high uint32.
func TestUniqueAddress_UIDSplit(t *testing.T) {
	uid := uint64(0x0000000100000002)
	ua := crdt.DDUniqueAddress{
		UIDLow:  uint32(uid & 0xFFFFFFFF),
		UIDHigh: uint32(uid >> 32),
	}
	if ua.UID() != uid {
		t.Errorf("UID() = %d, want %d", ua.UID(), uid)
	}
}

// TestUniqueAddress_FixedTagsInPayload verifies fixed32 tags 0x15 and 0x1d
// appear in the Scala GCounter fixture (uid_low and uid_high encoding).
func TestUniqueAddress_FixedTagsInPayload(t *testing.T) {
	data := loadFixtureCRDT(t, "ddata_gcounter.bin")
	// uid_low=11111=0x2B67 as LE fixed32: tag=0x15, bytes 0x67 0x2b 0x00 0x00
	wantLow := []byte{0x15, 0x67, 0x2b, 0x00, 0x00}
	if !bytes.Contains(data, wantLow) {
		t.Errorf("uid_low=11111 fixed32 %x not found in fixture %x", wantLow, data)
	}
	// uid_high=0 as LE fixed32: tag=0x1d, bytes 0x00 0x00 0x00 0x00
	wantHigh := []byte{0x1d, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Contains(data, wantHigh) {
		t.Errorf("uid_high=0 fixed32 %x not found in fixture %x", wantHigh, data)
	}
}

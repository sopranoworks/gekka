package sharding

import (
	"bytes"
	"testing"
)

func TestShardingSerializer_RoundTrip(t *testing.T) {
	s := &ShardingSerializer{}

	cases := []struct {
		name     string
		manifest string
		in       any
	}{
		{"Register", RegisterManifest, &PekkoSharding_Register{Ref: "pekko://Sys@h:1/user/region#42"}},
		{"RegisterAck", RegisterAckManifest, &PekkoSharding_RegisterAck{Ref: "pekko://Sys@h:1/system/sharding/foo/coordinator#7"}},
		{"GetShardHome", GetShardHomeManifest, &PekkoSharding_GetShardHome{Shard: "shard-3"}},
		{"ShardHome", ShardHomeManifest, &PekkoSharding_ShardHome{Shard: "shard-3", Region: "pekko://Sys@h:1/user/region#42"}},
		{"BeginHandOff", BeginHandOffManifest, &PekkoSharding_BeginHandOff{Shard: "shard-3"}},
		{"BeginHandOffAck", BeginHandOffAckManifest, &PekkoSharding_BeginHandOffAck{Shard: "shard-3"}},
		{"HandOff", HandOffManifest, &PekkoSharding_HandOff{Shard: "shard-3"}},
		{"ShardStopped", ShardStoppedManifest, &PekkoSharding_ShardStopped{Shard: "shard-3"}},
		{"StartEntity", StartEntityManifest, &PekkoSharding_StartEntity{EntityId: "e7"}},
		{"StartEntityAck", StartEntityAckManifest, &PekkoSharding_StartEntityAck{EntityId: "e7", ShardId: "shard-3"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bin, err := s.ToBinary(c.in)
			if err != nil {
				t.Fatalf("ToBinary: %v", err)
			}
			out, err := s.FromBinary(bin, c.manifest)
			if err != nil {
				t.Fatalf("FromBinary: %v", err)
			}
			// Re-serialize and compare byte-for-byte.
			bin2, err := s.ToBinary(out)
			if err != nil {
				t.Fatalf("ToBinary(round2): %v", err)
			}
			if !bytes.Equal(bin, bin2) {
				t.Fatalf("round-trip mismatch:\n  before: %x\n  after:  %x", bin, bin2)
			}
		})
	}
}

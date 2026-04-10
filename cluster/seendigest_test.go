/*
 * seendigest_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/stretchr/testify/assert"
)

func TestBuildSeenDigest(t *testing.T) {
	cm := &ClusterManager{State: &gproto_cluster.Gossip{
		AllAddresses: make([]*gproto_cluster.UniqueAddress, 10),
		Overview:     &gproto_cluster.GossipOverview{Seen: []int32{0, 3, 9}},
	}}
	digest := cm.buildSeenDigest()
	assert.Equal(t, 2, len(digest)) // ceil(10/8) = 2 bytes
	assert.Equal(t, byte(0b00001001), digest[0]) // bits 0 and 3
	assert.Equal(t, byte(0b00000010), digest[1]) // bit 9 (= bit 1 of byte 1)
}

func TestBuildSeenDigest_Empty(t *testing.T) {
	cm := &ClusterManager{State: &gproto_cluster.Gossip{}}
	digest := cm.buildSeenDigest()
	assert.Nil(t, digest)
}

func TestBuildSeenDigest_NilOverview(t *testing.T) {
	cm := &ClusterManager{State: &gproto_cluster.Gossip{
		AllAddresses: make([]*gproto_cluster.UniqueAddress, 4),
	}}
	digest := cm.buildSeenDigest()
	assert.Equal(t, 1, len(digest))
	assert.Equal(t, byte(0), digest[0])
}

func TestSeenDigestHasNew(t *testing.T) {
	assert.False(t, seenDigestHasNew([]byte{0b0101}, []byte{0b0111}))
	assert.True(t, seenDigestHasNew([]byte{0b0110}, []byte{0b0100}))
	assert.True(t, seenDigestHasNew([]byte{0b01, 0b10}, []byte{0b01}))
	assert.False(t, seenDigestHasNew(nil, nil))
	assert.False(t, seenDigestHasNew(nil, []byte{0xFF}))
}

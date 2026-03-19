/*
 * typed_singleton_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/stretchr/testify/assert"
)

func TestTypedSingleton(t *testing.T) {
	received := make(chan string, 10)
	behavior := func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
		received <- msg
		return typed.Same[string]()
	}

	// Mock environment
	cm := &ClusterManager{}
	cm.LocalAddress = &gproto_cluster.UniqueAddress{}
	
	singleton := NewTypedSingleton(cm, behavior, "")
	props := singleton.Props()
	
	assert.NotNil(t, props.New)
	
	// Spawning and message passing would require a full cluster setup which is
	// typically done in integration tests. Here we verify the structure.
	mgr := props.New().(*ClusterSingletonManager)
	assert.Equal(t, cm, mgr.cm)
}

func TestTypedSingletonProxy(t *testing.T) {
	cm := &ClusterManager{}
	proxy := NewTypedSingletonProxy[string](cm, nil, "/user/singletonManager", "")
	
	assert.Equal(t, "/user/singletonManager", proxy.Path())
	assert.NotNil(t, proxy.Untyped())
}

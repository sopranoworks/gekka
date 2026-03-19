/*
 * typed_singleton_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/cluster"
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
	cm := &cluster.ClusterManager{}
	cm.LocalAddress = &gproto_cluster.UniqueAddress{}
	
	singleton := NewTypedSingleton(cm, behavior, "")
	props := singleton.Props()
	
	assert.NotNil(t, props.New)
	
	// Spawning and message passing would require a full cluster setup which is
	// typically done in integration tests. Here we verify the structure.
	mgr := props.New().(*cluster.ClusterSingletonManager)
	assert.NotNil(t, mgr)
}

func TestTypedSingletonProxy(t *testing.T) {
	cm := &cluster.ClusterManager{}
	proxy := NewTypedSingletonProxy[string](cm, nil, "/user/singletonManager", "")
	
	assert.Equal(t, "/user/singletonManager", proxy.Path())
	assert.NotNil(t, proxy.Untyped())
}

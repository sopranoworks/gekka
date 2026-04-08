/*
 * extension_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync"
	"sync/atomic"
	"testing"
)

type testExtension struct {
	value string
}

func (*testExtension) isExtension() {}

type testExtId struct{}

func (testExtId) CreateExtension(sys ActorContext) Extension {
	return &testExtension{value: "initialized"}
}

func TestExtensionRegistry_RegisterAndGet(t *testing.T) {
	reg := NewExtensionRegistry(nil)
	id := testExtId{}

	ext := reg.RegisterExtension(id)
	if ext == nil {
		t.Fatal("expected non-nil extension")
	}

	te, ok := ext.(*testExtension)
	if !ok {
		t.Fatalf("expected *testExtension, got %T", ext)
	}
	if te.value != "initialized" {
		t.Errorf("got %q, want %q", te.value, "initialized")
	}

	// Get same instance
	ext2, found := reg.Extension(id)
	if !found {
		t.Fatal("expected to find registered extension")
	}
	if ext != ext2 {
		t.Error("expected same instance on second access")
	}
}

func TestExtensionRegistry_NotRegistered(t *testing.T) {
	reg := NewExtensionRegistry(nil)
	_, found := reg.Extension(testExtId{})
	if found {
		t.Error("expected not found for unregistered extension")
	}
}

func TestExtensionRegistry_ConcurrentAccess(t *testing.T) {
	var createCount atomic.Int32
	type concurrentExtId struct{}

	reg := NewExtensionRegistry(nil)
	id := concurrentExtId{}

	// Create extension that tracks creation count
	createExt := func(_ ActorContext) Extension {
		createCount.Add(1)
		return &testExtension{value: "concurrent"}
	}

	// Override: use a wrapper that implements ExtensionId
	wrapper := &funcExtensionId{create: createExt}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reg.RegisterExtension(wrapper)
		}()
	}
	wg.Wait()

	if count := createCount.Load(); count != 1 {
		t.Errorf("CreateExtension called %d times, want 1", count)
	}
	_ = id
}

type funcExtensionId struct {
	create func(ActorContext) Extension
}

func (f *funcExtensionId) CreateExtension(sys ActorContext) Extension {
	return f.create(sys)
}

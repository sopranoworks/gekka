/*
 * k8s_lease_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package k8s

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func testSettings(name, owner string) LeaseSettings {
	return LeaseSettings{
		LeaseName:     name,
		OwnerName:     owner,
		LeaseDuration: 15 * time.Second,
		RetryInterval: 100 * time.Millisecond,
	}
}

func TestKubernetesLease_AcquireCreatesLease(t *testing.T) {
	client := fake.NewClientset()
	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	ctx := context.Background()

	ok, err := l.Acquire(ctx, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Fatal("Acquire: expected true")
	}

	// Verify the Kubernetes Lease object was created with the correct holder.
	kl, err := client.CoordinationV1().Leases("default").Get(ctx, "test-lease", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after Acquire: %v", err)
	}
	if kl.Spec.HolderIdentity == nil || *kl.Spec.HolderIdentity != "node1" {
		t.Fatalf("expected holderIdentity=node1, got %v", kl.Spec.HolderIdentity)
	}
}

func TestKubernetesLease_CheckLease_True(t *testing.T) {
	client := fake.NewClientset()
	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	ctx := context.Background()

	if _, err := l.Acquire(ctx, nil); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !l.CheckLease() {
		t.Fatal("CheckLease: expected true after Acquire")
	}
}

func TestKubernetesLease_CheckLease_False_WhenHeldByOther(t *testing.T) {
	client := fake.NewClientset()
	// node2 acquires first.
	other := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node2"))
	ctx := context.Background()
	if _, err := other.Acquire(ctx, nil); err != nil {
		t.Fatalf("other.Acquire: %v", err)
	}

	// node1 tries to check — should see false.
	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	if l.CheckLease() {
		t.Fatal("CheckLease: expected false when lease is held by another node")
	}
}

func TestKubernetesLease_Acquire_ReturnsFalse_WhenHeldByOther(t *testing.T) {
	client := fake.NewClientset()
	other := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node2"))
	ctx := context.Background()
	if _, err := other.Acquire(ctx, nil); err != nil {
		t.Fatalf("other.Acquire: %v", err)
	}

	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	ok, err := l.Acquire(ctx, nil)
	if err != nil {
		t.Fatalf("Acquire: unexpected error: %v", err)
	}
	if ok {
		t.Fatal("Acquire: expected false when lease is held by another node")
	}
}

func TestKubernetesLease_Release(t *testing.T) {
	client := fake.NewClientset()
	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	ctx := context.Background()

	if _, err := l.Acquire(ctx, nil); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ok, err := l.Release(ctx)
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !ok {
		t.Fatal("Release: expected true (was held)")
	}
	if l.CheckLease() {
		t.Fatal("CheckLease: expected false after Release")
	}
}

func TestKubernetesLease_Release_NotHeld(t *testing.T) {
	client := fake.NewClientset()
	l := newKubernetesLeaseWithClient(client, "default", testSettings("test-lease", "node1"))
	ctx := context.Background()

	ok, err := l.Release(ctx)
	if err != nil {
		t.Fatalf("Release on non-held lease: unexpected error: %v", err)
	}
	if ok {
		t.Fatal("Release: expected false when lease was not held")
	}
}

func TestKubernetesLease_CheckLease_NoResource(t *testing.T) {
	client := fake.NewClientset()
	l := newKubernetesLeaseWithClient(client, "default", testSettings("nonexistent", "node1"))
	if l.CheckLease() {
		t.Fatal("CheckLease: expected false when Lease resource does not exist")
	}
}

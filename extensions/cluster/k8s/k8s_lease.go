/*
 * k8s_lease.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package k8s

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// LeaseSettings configures a KubernetesLease.
type LeaseSettings struct {
	// LeaseName is the name of the Kubernetes Lease resource.
	LeaseName string
	// OwnerName is the identity of this node (e.g. "host:port").
	OwnerName string
	// LeaseDuration is how long the lease is valid without renewal.
	// Defaults to 15s.
	LeaseDuration time.Duration
	// RetryInterval is how often the lost-watcher goroutine polls.
	// Defaults to 5s.
	RetryInterval time.Duration
}

// KubernetesLease implements a distributed lease backed by the Kubernetes
// coordination.k8s.io/v1 Lease resource.
//
// It satisfies cluster.LeaseChecker (CheckLease() bool) and can be passed
// directly to SBRConfig.Lease for the lease-majority SBR strategy.
//
// Acquire creates or updates the Lease object in the cluster, setting this
// node as the holder. Release clears the holderIdentity. CheckLease queries
// the current Lease and returns true when this node is listed as the holder.
//
// This mirrors the Pekko Kubernetes Lease implementation
// (pekko-lease-kubernetes).
type KubernetesLease struct {
	client    kubernetes.Interface
	settings  LeaseSettings
	namespace string

	mu   sync.Mutex
	held bool // optimistic local cache; authoritative answer comes from CheckLease
}

// NewKubernetesLease creates a KubernetesLease using the in-cluster
// configuration. namespace is the Kubernetes namespace where the Lease
// object lives (usually the same namespace as the pod).
func NewKubernetesLease(namespace string, settings LeaseSettings) (*KubernetesLease, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s lease: cannot build in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s lease: cannot create client: %w", err)
	}
	return newKubernetesLeaseWithClient(client, namespace, settings), nil
}

// newKubernetesLeaseWithClient is used by tests to inject a fake client.
func newKubernetesLeaseWithClient(client kubernetes.Interface, namespace string, settings LeaseSettings) *KubernetesLease {
	return &KubernetesLease{
		client:    client,
		settings:  settings,
		namespace: namespace,
	}
}

// Acquire attempts to acquire the Kubernetes Lease by setting holderIdentity
// to settings.OwnerName. If the Lease does not exist it is created. If it is
// already held by a different node, Acquire returns false without error.
//
// onLeaseLost is invoked in a goroutine if the lease is lost after a
// successful acquisition. Pass nil to disable.
func (l *KubernetesLease) Acquire(ctx context.Context, onLeaseLost func(error)) (bool, error) {
	leaseDuration := int32(l.settings.LeaseDuration.Seconds())
	if leaseDuration <= 0 {
		leaseDuration = 15
	}
	owner := l.settings.OwnerName
	now := metav1.NewMicroTime(time.Now())

	coordClient := l.client.CoordinationV1().Leases(l.namespace)

	existing, err := coordClient.Get(ctx, l.settings.LeaseName, metav1.GetOptions{})
	if err != nil {
		// Lease does not exist — create it.
		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      l.settings.LeaseName,
				Namespace: l.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &owner,
				LeaseDurationSeconds: &leaseDuration,
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}
		if _, createErr := coordClient.Create(ctx, newLease, metav1.CreateOptions{}); createErr != nil {
			return false, fmt.Errorf("k8s lease: create: %w", createErr)
		}
		l.mu.Lock()
		l.held = true
		l.mu.Unlock()
		l.startLostWatcher(ctx, onLeaseLost)
		return true, nil
	}

	// Lease exists — check whether it is free or held by us.
	if existing.Spec.HolderIdentity != nil &&
		*existing.Spec.HolderIdentity != "" &&
		*existing.Spec.HolderIdentity != owner {
		return false, nil // held by someone else
	}

	existing.Spec.HolderIdentity = &owner
	existing.Spec.LeaseDurationSeconds = &leaseDuration
	existing.Spec.RenewTime = &now
	if _, updateErr := coordClient.Update(ctx, existing, metav1.UpdateOptions{}); updateErr != nil {
		return false, fmt.Errorf("k8s lease: update: %w", updateErr)
	}

	l.mu.Lock()
	l.held = true
	l.mu.Unlock()
	l.startLostWatcher(ctx, onLeaseLost)
	return true, nil
}

// Release clears the holderIdentity on the Kubernetes Lease object.
func (l *KubernetesLease) Release(ctx context.Context) (bool, error) {
	l.mu.Lock()
	wasHeld := l.held
	l.held = false
	l.mu.Unlock()

	if !wasHeld {
		return false, nil
	}

	coordClient := l.client.CoordinationV1().Leases(l.namespace)
	existing, err := coordClient.Get(ctx, l.settings.LeaseName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("k8s lease: get for release: %w", err)
	}
	empty := ""
	existing.Spec.HolderIdentity = &empty
	if _, err = coordClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return false, fmt.Errorf("k8s lease: release update: %w", err)
	}
	return true, nil
}

// CheckLease queries the Kubernetes API and returns true when this node is
// listed as the holder of the Lease object.
func (l *KubernetesLease) CheckLease() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	coordClient := l.client.CoordinationV1().Leases(l.namespace)
	lease, err := coordClient.Get(ctx, l.settings.LeaseName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	if lease.Spec.HolderIdentity == nil {
		return false
	}
	return *lease.Spec.HolderIdentity == l.settings.OwnerName
}

// Settings returns the lease configuration.
func (l *KubernetesLease) Settings() LeaseSettings { return l.settings }

// startLostWatcher polls CheckLease at the retry interval and invokes
// onLeaseLost if the lease is lost after a successful Acquire.
func (l *KubernetesLease) startLostWatcher(ctx context.Context, onLeaseLost func(error)) {
	if onLeaseLost == nil {
		return
	}
	interval := l.settings.RetryInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !l.CheckLease() {
					l.mu.Lock()
					l.held = false
					l.mu.Unlock()
					onLeaseLost(fmt.Errorf("k8s lease lost: %q is now held by another node", l.settings.LeaseName))
					return
				}
			}
		}
	}()
}

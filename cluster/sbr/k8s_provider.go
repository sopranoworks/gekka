/*
 * k8s_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package sbr provides infrastructure-aware Split Brain Resolver helpers.
//
// The K8sProvider implements cluster.InfrastructureProvider by querying the
// Kubernetes API to confirm whether a cluster member's Pod is still running.
// When the SBR receives an UnreachableMember event it calls PodStatus; if the
// provider returns InfraDead the member is downed immediately without waiting
// for the stable-after timeout.
package sbr

import (
	"context"
	"fmt"

	"github.com/sopranoworks/gekka/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// K8sProviderConfig configures a K8sProvider.
type K8sProviderConfig struct {
	// Namespace is the Kubernetes namespace to search for Pods.
	// Defaults to "" which means all namespaces.
	Namespace string

	// LabelSelector filters Pods by label in addition to PodIP lookup.
	// Leave empty to search by IP only.
	LabelSelector string
}

// K8sProvider implements cluster.InfrastructureProvider using the Kubernetes
// API.  It looks up Pods by their IP address (which matches the Host field of
// a cluster MemberAddress when running in Kubernetes) and maps Pod lifecycle
// state to InfraStatus values.
type K8sProvider struct {
	clientset kubernetes.Interface
	cfg       K8sProviderConfig
}

// NewK8sProvider creates a K8sProvider using the in-cluster kubeconfig.
// Returns an error when the process is not running inside a Kubernetes cluster.
func NewK8sProvider(cfg K8sProviderConfig) (*K8sProvider, error) {
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("sbr/k8s: in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("sbr/k8s: clientset: %w", err)
	}
	return NewK8sProviderWithClientset(cs, cfg), nil
}

// NewK8sProviderWithClientset creates a K8sProvider with an injected clientset.
// Use this constructor in tests (pass fake.NewSimpleClientset()) or when you
// need to supply a pre-configured client from outside.
func NewK8sProviderWithClientset(cs kubernetes.Interface, cfg K8sProviderConfig) *K8sProvider {
	return &K8sProvider{clientset: cs, cfg: cfg}
}

// PodStatus queries the Kubernetes API to determine the liveness of the cluster
// member whose Pod is expected to run at address.Host.
//
// Lookup strategy:
//  1. List Pods in the configured namespace with a field selector for
//     status.podIP = address.Host.
//  2. No matching Pod → InfraDead (the Pod was deleted or never existed).
//  3. Pod has a non-nil DeletionTimestamp → InfraDead (being terminated).
//  4. Pod phase is Failed or Succeeded → InfraDead.
//  5. Any other phase (Running, Pending, Unknown) → InfraAlive (err on the
//     side of caution; the cluster FD may have a transient view).
//  6. API error or context timeout → InfraUnknown (let SBR fall back to the
//     stable-after timeout).
func (p *K8sProvider) PodStatus(ctx context.Context, address cluster.MemberAddress) cluster.InfraStatus {
	opts := metav1.ListOptions{
		FieldSelector: fmt.Sprintf("status.podIP=%s", address.Host),
	}
	if p.cfg.LabelSelector != "" {
		opts.LabelSelector = p.cfg.LabelSelector
	}

	pods, err := p.clientset.CoreV1().Pods(p.cfg.Namespace).List(ctx, opts)
	if err != nil {
		// Cannot reach the API or context timed out — be conservative.
		return cluster.InfraUnknown
	}
	if len(pods.Items) == 0 {
		// No Pod with this IP: it has been deleted or evicted.
		return cluster.InfraDead
	}

	// Examine the first matching Pod.  Multiple Pods sharing a PodIP would be
	// unusual (only possible transiently during IP reuse after deletion).
	pod := &pods.Items[0]

	// A non-nil DeletionTimestamp means Kubernetes has accepted the delete
	// request; the Pod is being terminated.
	if pod.DeletionTimestamp != nil {
		return cluster.InfraDead
	}

	switch pod.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return cluster.InfraDead
	}

	// Running, Pending, Unknown — treat as alive; the cluster FD may resolve.
	return cluster.InfraAlive
}

// Ensure K8sProvider implements cluster.InfrastructureProvider at compile time.
var _ cluster.InfrastructureProvider = (*K8sProvider)(nil)

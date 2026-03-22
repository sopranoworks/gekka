/*
 * api_provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAPIProvider_FetchSeedNodes(t *testing.T) {
	clientset := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "gekka"},
			},
			Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-2",
				Namespace: "default",
				Labels:    map[string]string{"app": "gekka"},
			},
			Status: corev1.PodStatus{
				PodIP: "10.0.0.2",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-3",
				Namespace: "default",
				Labels:    map[string]string{"app": "other"},
			},
			Status: corev1.PodStatus{
				PodIP: "10.0.0.3",
			},
		},
	)

	provider := NewAPIProvider(clientset, "default", "app=gekka", 2552)
	seeds, err := provider.FetchSeedNodes()

	assert.NoError(t, err)
	assert.Len(t, seeds, 2)
	assert.Contains(t, seeds, "10.0.0.1:2552")
	assert.Contains(t, seeds, "10.0.0.2:2552")
}

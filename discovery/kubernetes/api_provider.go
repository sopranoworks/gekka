package kubernetes

import (
	"context"
	"fmt"

	"github.com/sopranoworks/gekka/discovery"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func init() {
	discovery.Register("kubernetes-api", APIFactory)
}

// APIFactory creates a new Kubernetes APIProvider from generic DiscoveryConfig.
func APIFactory(config discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
	namespace, _ := config.Config["namespace"].(string)
	labelSelector, _ := config.Config["label-selector"].(string)
	port, _ := config.Config["port"].(int)

	// Attempt to build in-cluster config. This will fail if not running in K8s,
	// which is expected behavior for local development.
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("kubernetes-api: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("kubernetes-api: %w", err)
	}

	return NewAPIProvider(clientset, namespace, labelSelector, port), nil
}

// APIProvider implements discovery.SeedProvider using the Kubernetes API.
type APIProvider struct {
	clientset     kubernetes.Interface
	namespace     string
	labelSelector string
	port          int
}

// NewAPIProvider creates a new Kubernetes APIProvider.
func NewAPIProvider(clientset kubernetes.Interface, namespace, labelSelector string, port int) *APIProvider {
	return &APIProvider{
		clientset:     clientset,
		namespace:     namespace,
		labelSelector: labelSelector,
		port:          port,
	}
}

// FetchSeedNodes returns the IP addresses of the Pods matching the label selector.
func (p *APIProvider) FetchSeedNodes() ([]string, error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: p.labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods in namespace %s: %w", p.namespace, err)
	}

	var ips []string
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			if p.port > 0 {
				ips = append(ips, fmt.Sprintf("%s:%d", pod.Status.PodIP, p.port))
			} else {
				ips = append(ips, pod.Status.PodIP)
			}
		}
	}

	return ips, nil
}

// Ensure APIProvider implements SeedProvider.
var _ discovery.SeedProvider = (*APIProvider)(nil)

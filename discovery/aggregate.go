/*
 * aggregate.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"fmt"
	"strings"
)

// AggregateProvider merges results from multiple [SeedProvider] instances.
// It deduplicates results and tolerates individual provider failures as long
// as at least one provider succeeds.
type AggregateProvider struct {
	providers []SeedProvider
}

// NewAggregateProvider creates an [AggregateProvider] that queries all given
// providers and merges their results.
func NewAggregateProvider(providers ...SeedProvider) *AggregateProvider {
	return &AggregateProvider{providers: providers}
}

// FetchSeedNodes queries all providers and returns the deduplicated union of
// their results.  If some providers fail but at least one succeeds, the
// successful results are returned with a nil error.  If all providers fail,
// the combined errors are returned.
func (a *AggregateProvider) FetchSeedNodes() ([]string, error) {
	seen := make(map[string]struct{})
	var result []string
	var errs []string
	anySuccess := false

	for _, p := range a.providers {
		nodes, err := p.FetchSeedNodes()
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		anySuccess = true
		for _, n := range nodes {
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				result = append(result, n)
			}
		}
	}

	if !anySuccess && len(errs) > 0 {
		return nil, fmt.Errorf("aggregate discovery: all providers failed: %s", strings.Join(errs, "; "))
	}

	return result, nil
}

func init() {
	Register("aggregate", aggregateFactory)
}

// aggregateFactory wires `pekko.discovery.aggregate.discovery-methods` into
// a composed [AggregateProvider] over child providers resolved via the
// existing registry.  The same DiscoveryConfig is threaded down to each
// child so disjoint keys (`service-name`, `services`, `port`, ...) all
// resolve from the same flattened map populated by hocon_config.go.
func aggregateFactory(cfg DiscoveryConfig) (SeedProvider, error) {
	methods, err := extractDiscoveryMethods(cfg.Config["discovery-methods"])
	if err != nil {
		return nil, err
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("discovery: pekko.discovery.aggregate.discovery-methods must list at least one method")
	}
	children := make([]SeedProvider, 0, len(methods))
	for _, name := range methods {
		if name == "aggregate" {
			return nil, fmt.Errorf("discovery: pekko.discovery.aggregate.discovery-methods cannot include %q (would recurse)", name)
		}
		child, err := Get(name, cfg)
		if err != nil {
			return nil, fmt.Errorf("discovery: pekko.discovery.aggregate.discovery-methods: child %q: %w", name, err)
		}
		children = append(children, child)
	}
	return NewAggregateProvider(children...), nil
}

func extractDiscoveryMethods(v any) ([]string, error) {
	switch m := v.(type) {
	case nil:
		return nil, nil
	case []string:
		return m, nil
	case []any:
		out := make([]string, 0, len(m))
		for i, item := range m {
			s, ok := item.(string)
			if !ok || s == "" {
				return nil, fmt.Errorf("discovery: aggregate.discovery-methods[%d] must be a non-empty string", i)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("discovery: aggregate.discovery-methods must be a list of strings")
	}
}

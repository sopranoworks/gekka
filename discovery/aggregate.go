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
	Register("aggregate", func(config DiscoveryConfig) (SeedProvider, error) {
		// The aggregate provider is typically constructed programmatically
		// rather than via HOCON config.  This registration allows referencing
		// it by name for discovery method selection.
		return &AggregateProvider{}, nil
	})
}

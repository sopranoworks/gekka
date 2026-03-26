/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// ValidateConfig checks that all required Spanner configuration keys are
// present in cfg.  It is called automatically by the registered factories
// before any network I/O; use it directly when you need early validation:
//
//	if err := spannerstore.ValidateConfig(cfg); err != nil {
//	    log.Fatal(err)
//	}
func ValidateConfig(cfg hocon.Config) error {
	return persistence.ValidateProviderConfig(cfg, "project-id", "instance", "database")
}

// spannerDSN builds the Spanner database path from validated config.
// Precondition: ValidateConfig(cfg) == nil.
func spannerDSN(cfg hocon.Config) string {
	projectID, _ := cfg.GetString("project-id")
	inst, _ := cfg.GetString("instance")
	db, _ := cfg.GetString("database")
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, inst, db)
}

func init() {
	persistence.RegisterJournalProvider("spanner", func(cfg hocon.Config) (persistence.Journal, error) {
		if err := ValidateConfig(cfg); err != nil {
			return nil, err
		}
		dsn := spannerDSN(cfg)
		client, err := spanner.NewClient(context.Background(), dsn)
		if err != nil {
			return nil, fmt.Errorf("spannerstore: open spanner client for %s: %w", dsn, err)
		}
		return NewSpannerJournal(client, NewJSONCodec()), nil
	})

	persistence.RegisterSnapshotStoreProvider("spanner", func(cfg hocon.Config) (persistence.SnapshotStore, error) {
		if err := ValidateConfig(cfg); err != nil {
			return nil, err
		}
		dsn := spannerDSN(cfg)
		client, err := spanner.NewClient(context.Background(), dsn)
		if err != nil {
			return nil, fmt.Errorf("spannerstore: open spanner client for %s: %w", dsn, err)
		}
		return NewSpannerSnapshotStore(client, NewJSONCodec()), nil
	})

	persistence.RegisterReadJournalProvider("spanner", func(cfg hocon.Config) (any, error) {
		if err := ValidateConfig(cfg); err != nil {
			return nil, err
		}
		dsn := spannerDSN(cfg)
		client, err := spanner.NewClient(context.Background(), dsn)
		if err != nil {
			return nil, fmt.Errorf("spannerstore: open spanner client for %s: %w", dsn, err)
		}
		return NewSpannerReadJournal(client, NewJSONCodec()), nil
	})
}

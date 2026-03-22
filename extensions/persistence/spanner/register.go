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

// spannerFactory is the shared factory logic: reads project-id, instance, and
// database from cfg, opens a Spanner client, and passes it to constructor fn.
func spannerFactory(cfg hocon.Config, constructor func(*spanner.Client) persistence.Journal) (persistence.Journal, error) {
	projectID, _ := cfg.GetString("project-id")
	instance, _ := cfg.GetString("instance")
	database, _ := cfg.GetString("database")

	if projectID == "" || instance == "" || database == "" {
		return nil, fmt.Errorf(
			"spannerstore: persistence.journal.settings must define project-id, instance, and database; got project-id=%q instance=%q database=%q",
			projectID, instance, database,
		)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)
	client, err := spanner.NewClient(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("spannerstore: open spanner client for %s: %w", dsn, err)
	}
	return constructor(client), nil
}

func spannerSnapshotFactory(cfg hocon.Config) (persistence.SnapshotStore, error) {
	projectID, _ := cfg.GetString("project-id")
	instance, _ := cfg.GetString("instance")
	database, _ := cfg.GetString("database")

	if projectID == "" || instance == "" || database == "" {
		return nil, fmt.Errorf(
			"spannerstore: persistence.snapshot-store.settings must define project-id, instance, and database; got project-id=%q instance=%q database=%q",
			projectID, instance, database,
		)
	}

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)
	client, err := spanner.NewClient(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("spannerstore: open spanner client for %s: %w", dsn, err)
	}
	return NewSpannerSnapshotStore(client, NewJSONCodec()), nil
}

func init() {
	persistence.RegisterJournalProvider("spanner", func(cfg hocon.Config) (persistence.Journal, error) {
		return spannerFactory(cfg, func(c *spanner.Client) persistence.Journal {
			return NewSpannerJournal(c, NewJSONCodec())
		})
	})
	persistence.RegisterSnapshotStoreProvider("spanner", func(cfg hocon.Config) (persistence.SnapshotStore, error) {
		return spannerSnapshotFactory(cfg)
	})
}

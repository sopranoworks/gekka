/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cassandrastore

import (
	"fmt"

	"github.com/gocql/gocql"
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// DefaultCodec is the JSON codec used by all Cassandra providers.
// Users should register their domain types on it (e.g. via DefaultCodec.Register(...))
// before starting the actor system so that events and snapshots can be
// correctly serialized and deserialized.
var DefaultCodec = NewJSONCodec()

func init() {
	persistence.RegisterJournalProvider("cassandra", func(cfg hocon.Config) (persistence.Journal, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build journal: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: journal schema: %w", err)
		}
		return NewCassandraJournal(sess, c, DefaultCodec), nil
	})

	persistence.RegisterSnapshotStoreProvider("cassandra", func(cfg hocon.Config) (persistence.SnapshotStore, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build snapshot: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: snapshot schema: %w", err)
		}
		return NewCassandraSnapshotStore(sess, c, DefaultCodec), nil
	})

	persistence.RegisterDurableStateStoreProvider("cassandra", func(cfg hocon.Config) (persistence.DurableStateStore, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build state store: %w", err)
		}
		if err := CreateSchema(sess, c); err != nil {
			return nil, fmt.Errorf("cassandrastore: state store schema: %w", err)
		}
		return NewCassandraDurableStateStore(sess, c, DefaultCodec), nil
	})

	persistence.RegisterReadJournalProvider("cassandra", func(cfg hocon.Config) (any, error) {
		c, sess, err := buildFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cassandrastore: build read journal: %w", err)
		}
		return NewCassandraReadJournal(sess, c, DefaultCodec), nil
	})
}

func buildFromConfig(cfg hocon.Config) (*CassandraConfig, *gocql.Session, error) {
	c, err := ParseConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := GetOrCreateSession(c)
	if err != nil {
		return nil, nil, err
	}
	return c, sess, nil
}

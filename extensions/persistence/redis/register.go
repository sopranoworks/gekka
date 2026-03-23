/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package redisstore

import (
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// DefaultCodec is the shared JSON codec used by the factory-registered "redis"
// providers.  Register your domain event and snapshot types on this codec
// before calling gekka.NewActorSystem so that replayed payloads are returned
// as the correct Go types:
//
//	redisstore.DefaultCodec.Register(OrderPlaced{})
//	redisstore.DefaultCodec.Register(OrderShipped{})
var DefaultCodec = NewJSONCodec()

// ValidateConfig checks that all required Redis configuration keys are present
// in cfg.  It is called automatically by the registered factories before any
// network connection is attempted.
func ValidateConfig(cfg hocon.Config) error {
	return persistence.ValidateProviderConfig(cfg, "address")
}

// newClientFromConfig creates a *redis.Client from a HOCON sub-config.
// Supported keys (all under the settings block):
//
//	address    – host:port (required)
//	password   – authentication password (optional, default "")
//	db         – database index (optional, default 0)
//	key-prefix – string prepended to every Redis key (optional, default "gekka:")
func newClientFromConfig(cfg hocon.Config) (*redis.Client, string, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, "", err
	}

	addr, _ := cfg.GetString("address")

	password := ""
	if v, err := cfg.GetString("password"); err == nil {
		password = v
	}

	db := 0
	if v, err := cfg.GetString("db"); err == nil {
		if n, parseErr := strconv.Atoi(v); parseErr == nil {
			db = n
		}
	}
	if v, err := cfg.GetInt("db"); err == nil {
		db = v
	}

	keyPrefix := "gekka:"
	if v, err := cfg.GetString("key-prefix"); err == nil && v != "" {
		keyPrefix = v
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return client, keyPrefix, nil
}

// ParseConfigString is a convenience wrapper around hocon.ParseString for
// building a settings config in tests without importing the hocon package:
//
//	cfg, _ := redisstore.ParseConfigString(`{ address: "localhost:6379" }`)
//	j, _ := persistence.NewJournal("redis", *cfg)
func ParseConfigString(s string) (*hocon.Config, error) {
	return hocon.ParseString(s)
}

func init() {
	persistence.RegisterJournalProvider("redis", func(cfg hocon.Config) (persistence.Journal, error) {
		client, prefix, err := newClientFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("redisstore: build journal client: %w", err)
		}
		return NewRedisJournal(client, prefix, DefaultCodec), nil
	})

	persistence.RegisterSnapshotStoreProvider("redis", func(cfg hocon.Config) (persistence.SnapshotStore, error) {
		client, prefix, err := newClientFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("redisstore: build snapshot client: %w", err)
		}
		return NewRedisSnapshotStore(client, prefix, DefaultCodec), nil
	})
}

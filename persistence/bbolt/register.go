/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package bboltstore

import (
	"fmt"
	"path/filepath"
	"strings"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// DefaultCodec is the shared JSON codec used by the registered "bbolt" durable
// state store provider. Register your durable-state types on it before calling
// gekka.NewActorSystem so that state read back after a restart is returned as
// the correct Go type:
//
//	bboltstore.DefaultCodec.Register(AccountState{})
var DefaultCodec = NewJSONCodec()

// defaultStateFile is the file name used when the config supplies a directory
// ("dir") rather than an explicit file path ("path").
const defaultStateFile = "durable_state.db"

// pathFromConfig resolves the bbolt file path from a HOCON settings sub-config.
// It accepts either an explicit file "path" or a directory "dir" (in which case
// the file name defaults to durable_state.db). At least one must be set.
func pathFromConfig(cfg hocon.Config) (string, error) {
	if p, err := cfg.GetString("path"); err == nil && strings.TrimSpace(p) != "" {
		return p, nil
	}
	if d, err := cfg.GetString("dir"); err == nil && strings.TrimSpace(d) != "" {
		return filepath.Join(d, defaultStateFile), nil
	}
	return "", fmt.Errorf("bboltstore: missing required configuration: set either 'path' (a file) or 'dir' (a directory) under persistence.state-store.settings")
}

// ParseConfigString is a convenience wrapper around hocon.ParseString for
// building a settings config in tests without importing the hocon package:
//
//	cfg, _ := bboltstore.ParseConfigString(`{ path: "/tmp/state.db" }`)
//	ds, _ := persistence.NewDurableStateStore("bbolt", *cfg)
func ParseConfigString(s string) (*hocon.Config, error) {
	return hocon.ParseString(s)
}

func init() {
	persistence.RegisterDurableStateStoreProvider("bbolt", func(cfg hocon.Config) (persistence.DurableStateStore, error) {
		path, err := pathFromConfig(cfg)
		if err != nil {
			return nil, err
		}
		store, err := Open(path, DefaultCodec)
		if err != nil {
			return nil, fmt.Errorf("bboltstore: build durable state store: %w", err)
		}
		return store, nil
	})
}

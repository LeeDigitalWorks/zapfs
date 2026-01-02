// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migration represents a database migration
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// LoadMigrations loads all migration files from the embedded filesystem
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		// Parse version from filename: 001_create_objects.sql -> 1
		var version int
		var name string
		_, err := fmt.Sscanf(entry.Name(), "%d_%s", &version, &name)
		if err != nil {
			return nil, fmt.Errorf("parse migration filename %s: %w", entry.Name(), err)
		}

		// Read SQL content
		content, err := fs.ReadFile(migrationsFS, "migrations/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}

		migrations = append(migrations, Migration{
			Version: version,
			Name:    strings.TrimSuffix(name, ".sql"),
			SQL:     string(content),
		})
	}

	// Sort by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// Migrator handles database migrations
type Migrator interface {
	// CurrentVersion returns the current migration version
	CurrentVersion(ctx context.Context) (int, error)
	// Apply applies a migration
	Apply(ctx context.Context, m Migration) error
	// SetVersion records that a migration has been applied
	SetVersion(ctx context.Context, version int) error
}

// RunMigrations applies all pending migrations
func RunMigrations(ctx context.Context, migrator Migrator) error {
	migrations, err := LoadMigrations()
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	currentVersion, err := migrator.CurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("get current version: %w", err)
	}

	for _, m := range migrations {
		if m.Version <= currentVersion {
			continue
		}

		if err := migrator.Apply(ctx, m); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Name, err)
		}

		if err := migrator.SetVersion(ctx, m.Version); err != nil {
			return fmt.Errorf("set version %d: %w", m.Version, err)
		}
	}

	return nil
}

package db

import (
	"embed"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// RunMigrations applies all pending up migrations.
// SQL files are embedded into the binary — no external files needed at runtime.
func RunMigrations() error {
	src, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("iofs.New: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+buildURLDSN())
	if err != nil {
		return fmt.Errorf("migrate.New: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate.Up: %w", err)
	}

	return nil
}

func buildURLDSN() string {
	return fmt.Sprintf(
		"%s:%s@%s:%s/%s?sslmode=disable",
		envOrDefault("DB_USER", "postgres"),
		envOrDefault("DB_PASSWORD", "password123"),
		envOrDefault("DB_HOST", "localhost"),
		envOrDefault("DB_PORT", "5432"),
		envOrDefault("DB_NAME", "geofencing_db"),
	)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

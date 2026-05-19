package db

import (
	"embed"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/joho/godotenv"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// RunMigrations applies all pending up migrations.
// SQL files are embedded into the binary — no external files needed at runtime.
func RunMigrations() error {
	_ = godotenv.Load()
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
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)
}

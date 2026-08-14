package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	"github.com/pressly/goose/v3"
	_ "github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func runMigrations(db *sql.DB) error {
	migrationsFS, err := fs.Sub(embedMigrations, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationsFS)
	if err != nil {
		return err
	}
	if _, err := provider.Up(context.Background()); err != nil {
		return err
	}
	return nil
}

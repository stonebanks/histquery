package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stonebanks/histquery/internal/store"
)

type Repository struct {
	Db *sql.DB
}

func New(dbPath string) (*Repository, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating db directory: %w", err)
	}

	dsn := dbPath + "?_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}
	return &Repository{Db: db}, nil
}

func (r *Repository) Close() error {
	return r.Db.Close()
}

func (r *Repository) SaveEnrichedCommit(ctx context.Context, commits []store.EnrichedCommit) error {
	//TODO implement me
	panic("implement me")
}

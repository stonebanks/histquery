package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/stonebanks/histquery/internal/store"
	"github.com/stonebanks/histquery/internal/store/helpers"
	"github.com/stonebanks/histquery/internal/store/sqlite/db/sqlc"
)

type Store struct {
	queries *sqlc.Queries
	db      *sql.DB
}

func New(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating db directory: %w", err)
	}

	dsn := dbPath + "?_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if err := runMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}
	return &Store{
			queries: sqlc.New(db),
			db:      db,
		},
		nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) SaveEnrichedCommit(ctx context.Context, commits []store.EnrichedCommit) error {
	return s.execTx(ctx, func(q *sqlc.Queries) error {
		for _, c := range commits {
			err := q.InsertCommit(ctx, sqlc.InsertCommitParams{
				Sha:            c.Commit.SHA,
				AuthorName:     c.Commit.AuthorName,
				AuthorEmail:    helpers.ToNullString(c.Commit.AuthorEmail),
				AuthorDate:     helpers.ToNullTime(c.Commit.AuthorDate),
				CommitterName:  c.Commit.CommitterName,
				CommitterEmail: helpers.ToNullString(c.Commit.CommitterEmail),
				CommitterDate:  helpers.ToNullTime(c.Commit.CommitterDate),
				Message:        c.Commit.Body,
			})
			if err != nil {
				return err
			}

			err = q.InsertEmbedding(ctx, sqlc.InsertEmbeddingParams{
				CommitSha: c.Embedding.SHA,
				Source:    string(c.Embedding.Source),
				Model:     c.Embedding.Model,
				Dim:       int64(len(c.Embedding.Vector)),
				Vector:    helpers.Float32sToBytes(c.Embedding.Vector),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Store) MarkEmbeddingSynced(ctx context.Context, embed store.Embedding) error {
	return s.execTx(ctx, func(q *sqlc.Queries) error {
		if err := q.MarkEmbeddingSynced(ctx, sqlc.MarkEmbeddingSyncedParams{
			SyncedToChromemAt: helpers.ToNullTime(time.Now().UTC()),
			CommitSha:         embed.SHA,
			Source:            string(embed.Source),
			Model:             embed.Model,
		}); err != nil {
			return err
		}

		return nil
	})
}

func (s *Store) ListUnsyncedEmbeddings(ctx context.Context) ([]store.Embedding, error) {
	embeds, err := s.queries.ListUnsyncedEmbeddings(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]store.Embedding, len(embeds))
	for i, e := range embeds {
		vector, err := helpers.BytesToFloat32s(e.Vector)
		if err != nil {
			return nil, fmt.Errorf("decoding vector for commit %s: %w", e.CommitSha, err)
		}

		results[i] = store.Embedding{
			SHA:    e.CommitSha,
			Vector: vector,
			Model:  e.Model,
			Source: store.EmbeddingSource(e.Source),
		}
	}

	return results, nil
}

func (s *Store) execTx(ctx context.Context, fn func(*sqlc.Queries) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := sqlc.New(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("error: %w, rollback failed: %w", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

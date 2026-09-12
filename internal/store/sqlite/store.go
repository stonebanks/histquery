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

	// _texttotime + _time_format=datetime: without this, the driver writes DATETIME
	// via t.String() (e.g. "-0400 -0400" with a fixed zone) and Scan into time.Time fails on read
	dsn := dbPath + "?_pragma=foreign_keys(1)&_texttotime=1&_time_format=datetime"
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
				Sha:            c.Commit.SHA.String(),
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
				CommitSha: c.Embedding.SHA.String(),
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
			CommitSha:         embed.SHA.String(),
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

		id, err := store.NewCommitID(e.CommitSha)
		if err != nil {
			return nil, fmt.Errorf("decoding commit id for embedding: %w", err)
		}

		results[i] = store.Embedding{
			SHA:    id,
			Vector: vector,
			Model:  e.Model,
			Source: store.EmbeddingSource(e.Source),
		}
	}

	return results, nil
}

func (s *Store) ListCommitsById(ctx context.Context, shas []store.CommitID) ([]store.Commit, error) {
	if len(shas) == 0 {
		return []store.Commit{}, nil
	}

	stringShas := make([]string, len(shas))
	for i, sha := range shas {
		stringShas[i] = sha.String()
	}

	rows, err := s.queries.ListCommitsById(ctx, stringShas)
	if err != nil {
		return nil, err
	}

	bySha := make(map[store.CommitID]store.Commit, len(rows))
	for _, r := range rows {
		id, err := store.NewCommitID(r.Sha)
		if err != nil {
			return nil, fmt.Errorf("decoding commit id: %w", err)
		}

		bySha[id] = store.Commit{
			SHA:            id,
			Body:           r.Message,
			AuthorName:     r.AuthorName,
			AuthorEmail:    helpers.FromNullString(r.AuthorEmail),
			AuthorDate:     helpers.FromNullTime(r.AuthorDate),
			CommitterName:  r.CommitterName,
			CommitterEmail: helpers.FromNullString(r.CommitterEmail),
			CommitterDate:  helpers.FromNullTime(r.CommitterDate),
		}
	}

	results := make([]store.Commit, 0, len(shas))
	for _, sha := range shas {
		if c, ok := bySha[sha]; ok {
			results = append(results, c)
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

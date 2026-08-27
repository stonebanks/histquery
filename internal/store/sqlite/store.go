package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/stonebanks/histquery/internal/store"
	"github.com/stonebanks/histquery/internal/store/sqlite/db/sqlc"
)

type Store struct {
	*sqlc.Queries
	Db *sql.DB
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
			Queries: sqlc.New(db),
			Db:      db,
		},
		nil
}

func (s *Store) Close() error {
	return s.Db.Close()
}

func toNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func toNullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

func float32sToBytes(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

func (s *Store) SaveEnrichedCommit(ctx context.Context, commits []store.EnrichedCommit) error {
	return s.execTx(ctx, func(q *sqlc.Queries) error {
		for _, c := range commits {
			err := q.InsertCommit(ctx, sqlc.InsertCommitParams{
				Sha:            c.Commit.SHA,
				AuthorName:     c.Commit.AuthorName,
				AuthorEmail:    toNullString(c.Commit.AuthorEmail),
				AuthorDate:     toNullTime(c.Commit.AuthorDate),
				CommitterName:  c.Commit.CommitterName,
				CommitterEmail: toNullString(c.Commit.CommitterEmail),
				CommitterDate:  toNullTime(c.Commit.CommitterDate),
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
				Vector:    float32sToBytes(c.Embedding.Vector),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Store) execTx(ctx context.Context, fn func(*sqlc.Queries) error) error {
	tx, err := s.Db.BeginTx(ctx, nil)
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

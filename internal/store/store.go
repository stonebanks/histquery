package store

import (
	"context"
	"time"
)

type Store interface {
	SaveEnrichedCommit(ctx context.Context, commits []EnrichedCommit) error
	SearchCommitByQueryEmbedding(ctx context.Context, queryEmbedding []float32, opts *SearchByQueryOptions) ([]Commit, error)
}

type EmbeddingSyncer interface {
	MarkEmbeddingSynced(ctx context.Context, embed Embedding) error
	ListUnsyncedEmbeddings(ctx context.Context) ([]Embedding, error)
}

type CommitRetriever interface {
	ListCommitsById(ctx context.Context, commits []string) ([]Commit, error)
}

type PersistentStore interface {
	SaveEnrichedCommit(ctx context.Context, commits []EnrichedCommit) error
	Close() error
	EmbeddingSyncer
	CommitRetriever
}

type Commit struct {
	SHA            string
	Body           string
	AuthorName     string
	AuthorEmail    string
	AuthorDate     time.Time
	CommitterName  string
	CommitterEmail string
	CommitterDate  time.Time
}

type EmbeddingSource string

const (
	CommitMessage EmbeddingSource = "message"
	DiffSummary   EmbeddingSource = "diff_summary"
)

type Embedding struct {
	SHA    string
	Vector []float32
	Model  string
	Source EmbeddingSource
}

type EnrichedCommit struct {
	Commit    Commit
	Embedding Embedding
}

type SearchByQueryOptions struct {
	Take int
}

package store

import (
	"context"
	"time"
)

type Store interface {
	SaveEnrichedCommit(ctx context.Context, commits []EnrichedCommit) error
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

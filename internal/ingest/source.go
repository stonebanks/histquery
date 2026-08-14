package ingest

import (
	"context"
	"time"
)

type Source interface {
	StreamCommits(ctx context.Context, out chan<- Commit, errChan chan<- error)
}

type Developer struct {
	Name  string
	Email string
}

type Commit struct {
	SHA           string
	Body          string
	Author        Developer
	AuthorDate    time.Time
	Committer     Developer
	CommitterDate time.Time
}

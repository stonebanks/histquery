package ingest

import (
	"context"
	"time"
)

// Source streams commits into out, returning a fatal error if streaming
// could not complete. Implementations must not close out; the caller owns
// its lifetime. StreamCommits must return promptly when ctx is done.
type Source interface {
	StreamCommits(ctx context.Context, out chan<- Commit) error
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

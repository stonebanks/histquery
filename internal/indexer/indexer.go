package indexer

import (
	"context"

	"github.com/stonebanks/histquery/internal/ingest"
)

type Indexer struct {
	source ingest.Source
}

func New(source ingest.Source) *Indexer {
	return &Indexer{source: source}
}

func (i *Indexer) Run(ctx context.Context) error {
	commitsChan := make(chan ingest.Commit)
	errCh := make(chan error, 1)

	go i.source.StreamCommits(ctx, commitsChan, errCh)

	for {
		select {
		case _, ok := <-commitsChan:
			if !ok {
				return nil
			}
		case err := <-errCh:
			return err
		}
	}
}

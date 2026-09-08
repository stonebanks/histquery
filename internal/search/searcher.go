package search

import (
	"context"
	"fmt"

	"github.com/stonebanks/histquery/internal/embed"
	"github.com/stonebanks/histquery/internal/store"
)

type Searcher struct {
	embedder embed.Embedder
	store    store.Store
}

type Query string
type Options struct {
	TopK int
}

func New(embedder embed.Embedder, store store.Store) *Searcher {
	return &Searcher{
		embedder: embedder,
		store:    store,
	}
}

func (s *Searcher) Run(ctx context.Context, query Query, opts *Options) error {
	batch, err := s.embedder.EmbedBatch(ctx, []embed.Input{{
		ID:    "",
		Value: string(query),
	}})
	if err != nil {
		return fmt.Errorf("embedding query %w", err)
	}

	so := store.SearchByQueryOptions{
		Take: opts.TopK,
	}

	embedding, err := s.store.SearchCommitByQueryEmbedding(ctx, batch.Vector[0].Value, &so)
	if err != nil {
		return err
	}

	return nil
}

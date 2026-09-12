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

const (
	ansiBold  = "\033[1m"
	ansiReset = "\033[0m"
)

func (s *Searcher) Run(ctx context.Context, query Query, opts *Options) error {
	queryEmbedding, err := s.embedder.EmbedBatch(ctx, []embed.Input{{
		ID:    "",
		Value: string(query),
	}})
	if err != nil {
		return fmt.Errorf("embedding query %w", err)
	}

	searchOpts := store.SearchByOptions{
		Take: opts.TopK,
	}

	results, err := s.store.SearchSimilarCommits(ctx, queryEmbedding.Vector[0].Value, &searchOpts)
	if err != nil {
		return err
	}

	printResults(results)

	return nil
}

func printResults(results []store.SearchSimilarCommitsResult) {
	if len(results) == 0 {
		fmt.Println("No matching commits found.")
		return
	}

	for i, result := range results {
		fmt.Printf("%d. %s%s%s  (similarity: %.4f)\n   %s\n\n",
			i+1,
			ansiBold, shortSHA(result.Commit.SHA), ansiReset,
			result.Similarity,
			firstLine(result.Commit.Body),
		)
	}
}

func shortSHA(sha string) string {
	const shortLen = 7
	if len(sha) <= shortLen {
		return sha
	}
	return sha[:shortLen]
}

func firstLine(text string) string {
	for i, r := range text {
		if r == '\n' {
			return text[:i]
		}
	}
	return text
}

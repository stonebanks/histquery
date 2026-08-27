package indexer

import (
	"context"
	"errors"
	"fmt"

	"github.com/stonebanks/histquery/internal/embed"
	"github.com/stonebanks/histquery/internal/ingest"
	"github.com/stonebanks/histquery/internal/store"
)

type Indexer struct {
	source   ingest.Source
	embedder embed.Embedder
	store    store.Store
}

type StoreInput struct {
	commit           ingest.Commit
	messageEmbedding []float32
	model            embed.Model
}

type IndexerOptions struct {
	GetRepositoryPath string
}

func New(source ingest.Source, embedder embed.Embedder, store store.Store) *Indexer {
	return &Indexer{source: source, embedder: embedder, store: store}
}

const batchCommitToStoreSize = 50

func mapToEnrichedCommit(s []StoreInput) []store.EnrichedCommit {

	result := make([]store.EnrichedCommit, len(s))

	for i, v := range s {
		c := v.commit
		result[i] = store.EnrichedCommit{
			Commit: store.Commit{
				SHA:            c.SHA,
				Body:           c.Body,
				AuthorName:     c.Author.Name,
				AuthorEmail:    c.Author.Email,
				AuthorDate:     c.AuthorDate,
				CommitterName:  c.Committer.Name,
				CommitterEmail: c.Committer.Email,
				CommitterDate:  c.CommitterDate,
			},
			Embedding: store.Embedding{
				SHA:    c.SHA,
				Vector: v.messageEmbedding,
				Model:  string(v.model),
				Source: store.CommitMessage,
			},
		}
	}

	return result
}

func (idxr *Indexer) Run(ctx context.Context, options *IndexerOptions) error {
	commitsChan := make(chan ingest.Commit)
	sourceErrChan := make(chan error, 1)
	embedErrChan := make(chan error, 1)
	embeddingJobChan := make(chan []StoreInput, 1)

	go func() {
		defer close(commitsChan)
		if err := idxr.source.StreamCommits(ctx, commitsChan); err != nil {
			select {
			case sourceErrChan <- err:
			case <-ctx.Done():
			}
		}
	}()

	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		batchInputs := make([]embed.Input, 0, batchCommitToStoreSize)
		commitByID := make(map[string]ingest.Commit)

		for {
			select {
			case <-ctx.Done():
				return
			case commit, ok := <-commitsChan:
				if !ok {
					if len(batchInputs) > 0 {
						embeddingResult, err := idxr.embedder.EmbedBatch(ctx, batchInputs)
						if err != nil {
							select {
							case embedErrChan <- err:
							case <-ctx.Done():
							}
							return
						}

						si := make([]StoreInput, len(embeddingResult.Vector))
						for i, v := range embeddingResult.Vector {
							si[i] = StoreInput{commit: commitByID[v.ID], messageEmbedding: v.Value, model: embeddingResult.Model}
						}

						select {
						case embeddingJobChan <- si:
						case <-ctx.Done():
						}

					}

					return
				}

				commitByID[commit.SHA] = commit
				batchInputs = append(batchInputs, embed.Input{ID: commit.SHA, Value: commit.Body})

				if len(batchInputs) == batchCommitToStoreSize {
					embeddingResult, err := idxr.embedder.EmbedBatch(ctx, batchInputs)
					if err != nil {
						select {
						case embedErrChan <- err:
						case <-ctx.Done():
						}
						batchInputs = batchInputs[:0]
						continue
					}

					si := make([]StoreInput, len(embeddingResult.Vector))
					for i, v := range embeddingResult.Vector {
						si[i] = StoreInput{commit: commitByID[v.ID], messageEmbedding: v.Value, model: embeddingResult.Model}
					}

					select {
					case embeddingJobChan <- si:
					case <-ctx.Done():
					}

					// reset batch
					batchInputs = batchInputs[:0]
					si = si[:0]
				}
			}
		}
	}()

	go func() {
		<-dispatchDone
		close(embeddingJobChan)
	}()

	var errs []error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sourceErrChan:
			return fmt.Errorf("streaming commits: %w", err)
		case err := <-embedErrChan:
			errs = append(errs, fmt.Errorf("embedding batch: %w", err))
		case job, ok := <-embeddingJobChan:
			if !ok {
				if len(errs) > 0 {
					return errors.Join(errs...)
				}
				return nil
			}

			if err := idxr.store.SaveEnrichedCommit(ctx, mapToEnrichedCommit(job)); err != nil {
				errs = append(errs, fmt.Errorf("storing commit batch: %w", err))
			}
		}
	}
}

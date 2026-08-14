package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stonebanks/histquery/internal/embed"
	"github.com/stonebanks/histquery/internal/ingest"
	"github.com/stonebanks/histquery/internal/store"
)

type EmbeddingJob struct {
	commit           ingest.Commit
	messageEmbedding embed.CommitMessageEmbeddingResult
	err              error
}

type Indexer struct {
	source   ingest.Source
	embedder embed.Embedder
	store    store.Store
}

func New(source ingest.Source, embedder embed.Embedder, store store.Store) *Indexer {
	return &Indexer{source: source, embedder: embedder, store: store}
}

// TODO: Should be somehow related to OLLAMA_NUM_PARALLEL which is 1 by default (cf. https://docs.ollama.com/faq)
const defaultEmbedConcurrency = 2
const batchCommitToStoreSize = 50

func (idxr *Indexer) processCommit(
	ctx context.Context,
	commit ingest.Commit,
	embeddingJobChan chan<- EmbeddingJob,
	wg *sync.WaitGroup,
	semaphore chan struct{}) {

	defer wg.Done()

	select {
	case semaphore <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-semaphore }()

	r, err := idxr.embedder.Embed(ctx, commit.Body)

	select {
	case embeddingJobChan <- EmbeddingJob{commit: commit, messageEmbedding: r, err: err}:
	case <-ctx.Done():
	}
}

func mapToEnrichedCommit(e EmbeddingJob) store.EnrichedCommit {
	c := e.commit
	return store.EnrichedCommit{
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
			Vector: e.messageEmbedding.Vector,
			Model:  e.messageEmbedding.Model,
			Source: store.CommitMessage,
		},
	}
}

func (idxr *Indexer) Run(ctx context.Context) error {
	commitsChan := make(chan ingest.Commit)
	sourceErrChan := make(chan error, 1)
	embeddingJobChan := make(chan EmbeddingJob, 50)

	// TODO: Handle cancellation

	semaphore := make(chan struct{}, defaultEmbedConcurrency)
	var wg sync.WaitGroup

	go func() {
		wg.Wait()
		close(embeddingJobChan)
	}()

	go idxr.source.StreamCommits(ctx, commitsChan, sourceErrChan)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case commit, ok := <-commitsChan:
				if !ok {
					return
				}
				wg.Add(1)
				go idxr.processCommit(ctx, commit, embeddingJobChan, &wg, semaphore)
			}
		}
	}()

	batch := make([]store.EnrichedCommit, 0, batchCommitToStoreSize)

	var errs []error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sourceErrChan:
			return fmt.Errorf("streaming commits: %w", err)
		case job, ok := <-embeddingJobChan:
			if !ok {
				if len(batch) > 0 {
					if err := idxr.store.SaveEnrichedCommit(ctx, batch); err != nil {
						errs = append(errs, fmt.Errorf("storing commit batch: %w", err))
					}
				}
				if len(errs) > 0 {
					return errors.Join(errs...)
				}
				return nil
			}
			if job.err != nil {
				errs = append(errs, fmt.Errorf("embedding commit %s: %w", job.commit.SHA, job.err))
				continue
			}

			batch = append(batch, mapToEnrichedCommit(job))
			if len(batch) == batchCommitToStoreSize {
				if err := idxr.store.SaveEnrichedCommit(ctx, batch); err != nil {
					errs = append(errs, fmt.Errorf("storing commit batch: %w", err))
				}

				// reset batch
				batch = batch[:0]
			}

		}
	}
}

package syncstore

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"github.com/philippgille/chromem-go"
	"github.com/stonebanks/histquery/internal/store"
)

type Store struct {
	db         store.PersistentStore
	vectorDb   *chromem.Collection
	workerChan chan []chromem.Document
	stop       context.CancelFunc
	stopped    context.Context
	wg         *sync.WaitGroup
}

const (
	cst_commitSHA = "commitID"
	cst_source    = "source"
	cst_model     = "model"
)

func New(ctx context.Context, sqliteStore store.PersistentStore, chromemPath string) (*Store, error) {
	wg := sync.WaitGroup{}
	workerChan := make(chan []chromem.Document)
	workerCtx, cancel := context.WithCancel(ctx)

	cDb, err := chromem.NewPersistentDB(chromemPath, false)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating db: %w", err)
	}

	embeddingFunc := func(ctx context.Context, text string) ([]float32, error) {
		// we are not using chromem-go for embedding generation
		panic(nil)
	}

	c, err := cDb.CreateCollection("commits", nil, embeddingFunc)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating collection: %w", err)
	}

	wg.Add(1)
	go func(c *chromem.Collection, db store.EmbeddingSyncer, ch chan []chromem.Document) {
		defer wg.Done()

		for {
			select {
			case <-workerCtx.Done():
				return
			case docs := <-ch:
				addDocConcurrency := min(len(docs), runtime.NumCPU())
				if err := c.AddDocuments(ctx, docs, addDocConcurrency); err != nil {
					slog.Error("adding documents:", "error", err)
					continue
				}

				for _, doc := range docs {
					if err := db.MarkEmbeddingSynced(ctx, store.Embedding{
						SHA:    doc.Metadata[cst_commitSHA],
						Model:  doc.Metadata[cst_model],
						Source: store.EmbeddingSource(doc.Metadata[cst_source]),
					}); err != nil {
						slog.Error("updating entries in sqlite:", "error", err)
					}
				}
			}
		}
	}(c, sqliteStore, workerChan)

	wg.Add(1)
	go func(db store.EmbeddingSyncer, ch chan []chromem.Document) {
		defer wg.Done()
		unsynced, err := db.ListUnsyncedEmbeddings(ctx)
		if err != nil {
			slog.Error("listing unsynced embeddings:", "error", err)
			return
		}

		if len(unsynced) > 0 {
			documents := make([]chromem.Document, len(unsynced))
			for i, e := range unsynced {
				docID := docIDFrom(e)
				m := make(map[string]string)
				m[cst_commitSHA] = e.SHA
				m[cst_source] = string(e.Source)
				m[cst_model] = e.Model

				documents[i] = chromem.Document{
					ID:        docID,
					Metadata:  m,
					Embedding: e.Vector,
				}
			}

			select {
			case ch <- documents:
			case <-workerCtx.Done():
				return
			}
		}
	}(sqliteStore, workerChan)

	return &Store{
		db:         sqliteStore,
		vectorDb:   c,
		workerChan: workerChan,
		stop:       cancel,
		stopped:    workerCtx,
		wg:         &wg,
	}, nil
}

func (s *Store) Close() error {
	s.stop()
	s.wg.Wait()
	err := s.db.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) SaveEnrichedCommit(ctx context.Context, commits []store.EnrichedCommit) error {
	if len(commits) == 0 {
		return nil
	}

	if err := s.db.SaveEnrichedCommit(ctx, commits); err != nil {
		return fmt.Errorf("saving enriched commits: %w", err)
	}

	documents := make([]chromem.Document, len(commits))
	for i, commit := range commits {
		docID := docIDFrom(commit.Embedding)
		m := make(map[string]string)
		m[cst_commitSHA] = commit.Embedding.SHA
		m[cst_source] = string(commit.Embedding.Source)
		m[cst_model] = commit.Embedding.Model

		documents[i] = chromem.Document{
			ID:        docID,
			Metadata:  m,
			Embedding: commit.Embedding.Vector,
		}
	}

	select {
	case s.workerChan <- documents:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped.Done():
		return s.stopped.Err()
	}

	return nil
}

func (s *Store) SearchCommitByQueryEmbedding(ctx context.Context, queryEmbedding []float32, opts *store.SearchByQueryOptions) ([]store.Commit, error) {
	results, err := s.vectorDb.QueryEmbedding(ctx, queryEmbedding, opts.Take, nil, nil)
	if err != nil {
		return nil, err
	}

	type tLookup struct {
		ID         string
		similarity float32
	}

	idSimilarity := make([]tLookup, len(results))
	lookup := make([]string, len(results))
	for i, r := range results {
		idSimilarity[i] = tLookup{ID: r.Metadata[cst_commitSHA], similarity: r.Similarity}
		lookup[i] = r.Metadata[cst_commitSHA]
	}

	hydrated, err := s.db.ListCommitsById(ctx, lookup)
	if err != nil {
		return nil, fmt.Errorf("getting commit batch: %w", err)
	}

	return hydrated, nil
}

func docIDFrom(embedding store.Embedding) string {
	return embedding.SHA + "|" + embedding.Model + "|" + string(embedding.Source)
}

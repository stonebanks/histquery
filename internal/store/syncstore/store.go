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

	c, err := cDb.GetOrCreateCollection("commits", nil, embeddingFunc)
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
					id, err := store.NewCommitID(doc.Metadata[cst_commitSHA])
					if err != nil {
						slog.Error("invalid commit id in synced doc:", "error", err)
						continue
					}

					if err := db.MarkEmbeddingSynced(ctx, store.Embedding{
						SHA:    id,
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
				m[cst_commitSHA] = e.SHA.String()
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
		m[cst_commitSHA] = commit.Embedding.SHA.String()
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

func (s *Store) SearchSimilarCommits(ctx context.Context, queryEmbedding []float32, opts *store.SearchByOptions) ([]store.SearchSimilarCommitsResult, error) {
	take := opts.Take
	if count := s.vectorDb.Count(); take > count {
		take = count
	}
	if take <= 0 {
		return nil, nil
	}

	hits, err := s.vectorDb.QueryEmbedding(ctx, queryEmbedding, take, nil, nil)
	if err != nil {
		return nil, err
	}

	similarityBySHA := make(map[store.CommitID]float32, len(hits))
	shas := make([]store.CommitID, len(hits))
	for i, m := range hits {
		sha, err := store.NewCommitID(m.Metadata[cst_commitSHA])
		if err != nil {
			return nil, fmt.Errorf("invalid commit id in search hit: %w", err)
		}
		similarityBySHA[sha] = m.Similarity
		shas[i] = sha
	}

	commits, err := s.db.ListCommitsById(ctx, shas)
	if err != nil {
		return nil, fmt.Errorf("getting commit batch: %w", err)
	}

	results := make([]store.SearchSimilarCommitsResult, len(commits))
	for i, c := range commits {
		results[i] = store.SearchSimilarCommitsResult{
			Commit:     c,
			Similarity: similarityBySHA[c.SHA],
		}
	}

	return results, nil
}

func docIDFrom(embedding store.Embedding) string {
	return embedding.SHA.String() + "|" + embedding.Model + "|" + string(embedding.Source)
}

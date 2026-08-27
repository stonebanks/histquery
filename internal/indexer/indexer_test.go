package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stonebanks/histquery/internal/embed"
	"github.com/stonebanks/histquery/internal/ingest"
	"github.com/stonebanks/histquery/internal/store"
)

type fakeSource struct {
	commits []ingest.Commit
	err     error
}

func (f *fakeSource) StreamCommits(ctx context.Context, out chan<- ingest.Commit) error {
	for _, c := range f.commits {
		select {
		case out <- c:
		case <-ctx.Done():
			return nil
		}
	}
	return f.err
}

type fakeEmbedder struct {
	embedFn func(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error)

	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (f *fakeEmbedder) EmbedBatch(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
	cur := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)

	for {
		max := f.maxInFlight.Load()
		if cur <= max || f.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}

	return f.embedFn(ctx, inputs)
}

type fakeStore struct {
	mu      sync.Mutex
	batches [][]store.EnrichedCommit
	saveErr error
}

func (f *fakeStore) SaveEnrichedCommit(ctx context.Context, commits []store.EnrichedCommit) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch := make([]store.EnrichedCommit, len(commits))
	copy(batch, commits)
	f.batches = append(f.batches, batch)

	return f.saveErr
}

func makeCommits(n int) []ingest.Commit {
	commits := make([]ingest.Commit, n)
	for i := range commits {
		commits[i] = ingest.Commit{SHA: fmt.Sprintf("sha-%d", i), Body: fmt.Sprintf("commit body %d", i)}
	}
	return commits
}

// fixedEmbedder returns one deterministic vector per input, tagged with the
// input's ID so the indexer's SHA -> commit lookup resolves correctly.
func fixedEmbedder() *fakeEmbedder {
	return &fakeEmbedder{
		embedFn: func(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
			results := make([]embed.Embeddings, len(inputs))
			for i, in := range inputs {
				results[i] = embed.Embeddings{ID: in.ID, Value: []float32{0.1, 0.2}}
			}
			return embed.CommitMessageEmbeddingResult{Vector: results, Model: "fake-model"}, nil
		},
	}
}

func TestRun_PartialBatch(t *testing.T) {
	authorDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	committerDate := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	commit := ingest.Commit{
		SHA:           "abc123",
		Body:          "fix: something",
		Author:        ingest.Developer{Name: "Ada", Email: "ada@example.com"},
		AuthorDate:    authorDate,
		Committer:     ingest.Developer{Name: "Bob", Email: "bob@example.com"},
		CommitterDate: committerDate,
	}
	source := &fakeSource{commits: []ingest.Commit{commit}}
	embedder := fixedEmbedder()
	st := &fakeStore{}

	idxr := New(source, embedder, st)
	if err := idxr.Run(context.Background(), &IndexerOptions{}); err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}

	if len(st.batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(st.batches))
	}
	if len(st.batches[0]) != 1 {
		t.Fatalf("got %d commits in batch, want 1", len(st.batches[0]))
	}

	got := st.batches[0][0]
	want := store.EnrichedCommit{
		Commit: store.Commit{
			SHA:            "abc123",
			Body:           "fix: something",
			AuthorName:     "Ada",
			AuthorEmail:    "ada@example.com",
			AuthorDate:     authorDate,
			CommitterName:  "Bob",
			CommitterEmail: "bob@example.com",
			CommitterDate:  committerDate,
		},
		Embedding: store.Embedding{
			SHA:    "abc123",
			Vector: []float32{0.1, 0.2},
			Model:  "fake-model",
			Source: store.CommitMessage,
		},
	}
	if got.Commit != want.Commit {
		t.Errorf("Commit = %+v, want %+v", got.Commit, want.Commit)
	}
	if got.Embedding.SHA != want.Embedding.SHA || got.Embedding.Model != want.Embedding.Model || got.Embedding.Source != want.Embedding.Source {
		t.Errorf("Embedding = %+v, want %+v", got.Embedding, want.Embedding)
	}
	if len(got.Embedding.Vector) != len(want.Embedding.Vector) {
		t.Errorf("Embedding.Vector = %v, want %v", got.Embedding.Vector, want.Embedding.Vector)
	}
}

func TestRun_ExactBatchBoundary(t *testing.T) {
	source := &fakeSource{commits: makeCommits(batchCommitToStoreSize)}
	embedder := fixedEmbedder()
	st := &fakeStore{}

	idxr := New(source, embedder, st)
	if err := idxr.Run(context.Background(), &IndexerOptions{}); err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}

	if len(st.batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(st.batches))
	}
	if len(st.batches[0]) != batchCommitToStoreSize {
		t.Errorf("got %d commits in batch, want %d", len(st.batches[0]), batchCommitToStoreSize)
	}
}

func TestRun_BatchBoundaryPlusRemainder(t *testing.T) {
	source := &fakeSource{commits: makeCommits(batchCommitToStoreSize + 1)}
	embedder := fixedEmbedder()
	st := &fakeStore{}

	idxr := New(source, embedder, st)
	if err := idxr.Run(context.Background(), &IndexerOptions{}); err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}

	if len(st.batches) != 2 {
		t.Fatalf("got %d batches, want 2", len(st.batches))
	}
	if len(st.batches[0]) != batchCommitToStoreSize {
		t.Errorf("first batch has %d commits, want %d", len(st.batches[0]), batchCommitToStoreSize)
	}
	if len(st.batches[1]) != 1 {
		t.Errorf("second batch has %d commits, want 1", len(st.batches[1]))
	}
}

// TestRun_EmbedError verifies that a failed EmbedBatch call fails the whole
// batch (not just one commit) - embedding is one HTTP call per batch, so
// there's no per-commit granularity to fail independently.
func TestRun_EmbedError(t *testing.T) {
	commits := makeCommits(3)
	embedErr := errors.New("embed boom")

	source := &fakeSource{commits: commits}
	embedder := &fakeEmbedder{
		embedFn: func(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
			return embed.CommitMessageEmbeddingResult{}, embedErr
		},
	}
	st := &fakeStore{}

	idxr := New(source, embedder, st)
	err := idxr.Run(context.Background(), &IndexerOptions{})

	if err == nil {
		t.Fatal("Run() returned nil error, want error wrapping embed failure")
	}
	if !errors.Is(err, embedErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, embedErr)
	}

	if len(st.batches) != 0 {
		t.Fatalf("got %d batches stored, want 0 (whole batch excluded on embed failure)", len(st.batches))
	}
}

func TestRun_StoreError(t *testing.T) {
	saveErr := errors.New("store boom")

	source := &fakeSource{commits: makeCommits(batchCommitToStoreSize + 1)}
	embedder := fixedEmbedder()
	st := &fakeStore{saveErr: saveErr}

	idxr := New(source, embedder, st)
	err := idxr.Run(context.Background(), &IndexerOptions{})

	if err == nil {
		t.Fatal("Run() returned nil error, want error wrapping store failure")
	}
	if !errors.Is(err, saveErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, saveErr)
	}

	if len(st.batches) != 2 {
		t.Fatalf("got %d SaveEnrichedCommit calls, want 2 (both batches attempted despite error)", len(st.batches))
	}
	if len(st.batches[0]) != batchCommitToStoreSize {
		t.Errorf("first batch has %d commits, want %d", len(st.batches[0]), batchCommitToStoreSize)
	}
	if len(st.batches[1]) != 1 {
		t.Errorf("second batch has %d commits, want 1", len(st.batches[1]))
	}
}

func TestRun_SourceError(t *testing.T) {
	sourceErr := errors.New("source boom")

	source := &fakeSource{commits: makeCommits(3), err: sourceErr}
	embedder := fixedEmbedder()
	st := &fakeStore{}

	idxr := New(source, embedder, st)

	done := make(chan error, 1)
	go func() { done <- idxr.Run(context.Background(), &IndexerOptions{}) }()

	select {
	case err := <-done:
		if !errors.Is(err, sourceErr) {
			t.Errorf("Run() error = %v, want it to wrap %v", err, sourceErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return in time, want prompt return on source error")
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	source := &fakeSource{commits: makeCommits(1000)}
	embedder := &fakeEmbedder{
		embedFn: func(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
			time.Sleep(5 * time.Millisecond)
			results := make([]embed.Embeddings, len(inputs))
			for i, in := range inputs {
				results[i] = embed.Embeddings{ID: in.ID, Value: []float32{0.1}}
			}
			return embed.CommitMessageEmbeddingResult{Vector: results, Model: "fake-model"}, nil
		},
	}
	st := &fakeStore{}

	idxr := New(source, embedder, st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- idxr.Run(ctx, &IndexerOptions{}) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation, want prompt return (possible goroutine leak/deadlock)")
	}
}

// TestRun_EmbedCallsSequential locks in the current dispatcher design: batches
// are embedded one at a time in a single goroutine, never concurrently.
func TestRun_EmbedCallsSequential(t *testing.T) {
	source := &fakeSource{commits: makeCommits(batchCommitToStoreSize * 2)}
	embedder := &fakeEmbedder{
		embedFn: func(ctx context.Context, inputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
			time.Sleep(5 * time.Millisecond)
			results := make([]embed.Embeddings, len(inputs))
			for i, in := range inputs {
				results[i] = embed.Embeddings{ID: in.ID, Value: []float32{1}}
			}
			return embed.CommitMessageEmbeddingResult{Vector: results, Model: "fake-model"}, nil
		},
	}
	st := &fakeStore{}

	idxr := New(source, embedder, st)

	if err := idxr.Run(context.Background(), &IndexerOptions{}); err != nil {
		t.Fatalf("Run() returned error: %v", err)
	}

	if max := embedder.maxInFlight.Load(); max > 1 {
		t.Errorf("max concurrent EmbedBatch calls = %d, want <= 1 (batches are dispatched sequentially)", max)
	}
}

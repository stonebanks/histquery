package embed

import "context"

type CommitMessageEmbeddingResult struct {
	Vector []float32
	Model  string
}

type Embedder interface {
	Embed(ctx context.Context, str string) (CommitMessageEmbeddingResult, error)
}

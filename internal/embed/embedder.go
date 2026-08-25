package embed

import "context"

type Model string
type Input string
type Embeddings []float32

type CommitMessageEmbeddingResult struct {
	Vector []Embeddings
	Model  Model
}

type Embedder interface {
	EmbedBatch(ctx context.Context, batchInputs []Input) (CommitMessageEmbeddingResult, error)
}

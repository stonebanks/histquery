package embed

import "context"

type Model string
type Input struct {
	ID    string
	Value string
}
type Embeddings struct {
	ID    string
	Value []float32
}

type CommitMessageEmbeddingResult struct {
	Vector []Embeddings
	Model  Model
}

type Embedder interface {
	EmbedBatch(ctx context.Context, batchInputs []Input) (CommitMessageEmbeddingResult, error)
}

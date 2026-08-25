package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/stonebanks/histquery/internal/embed"
)

const (
	defaultBaseURL = "http://localhost:11434"
	defaultModel   = "nomic-embed-text"
)

type Embedder struct {
	baseURL string
	model   string
	client  *http.Client
}

func New(model *embed.Model) *Embedder {

	var m = defaultModel
	if model != nil {
		m = string(*model)
	}

	return &Embedder{
		baseURL: defaultBaseURL,
		model:   m,
		client:  http.DefaultClient,
	}
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Model      string      `json:"model"`
	Embeddings [][]float32 `json:"embeddings"`
}

func (e *Embedder) EmbedBatch(ctx context.Context, batchInputs []embed.Input) (embed.CommitMessageEmbeddingResult, error) {
	inputs := make([]string, len(batchInputs))
	for i, v := range batchInputs {
		inputs[i] = string(v)
	}
	body, err := json.Marshal(embedRequest{Model: e.model, Input: inputs})
	if err != nil {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("calling ollama: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("ollama returned status %d", resp.StatusCode)
	}

	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("decoding response: %w", err)
	}
	if len(out.Embeddings) == 0 {
		return embed.CommitMessageEmbeddingResult{}, fmt.Errorf("ollama returned no embeddings")
	}

	results := make([]embed.Embeddings, len(out.Embeddings))
	for i, v := range out.Embeddings {
		results[i] = v
	}

	return embed.CommitMessageEmbeddingResult{
		Vector: results,
		Model:  embed.Model(out.Model),
	}, nil
}

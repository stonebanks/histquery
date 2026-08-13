package embed

import "context"

type Embedder interface {
	Embed(ctx context.Context, str string)
}

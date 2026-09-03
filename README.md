# histquery
Self-hosted CLI that turns git commit history into a searchable memory **_(Project in still in development)_**.

## dependencies 

### Ollama
Runs an embedding model locally. By default `histquery` is using `nomic-embed-text`, which is lightweight and CPU-friendly.

```bash
$ ollama pull nomic-embed-text # pull embedding model
```

## build 

```bash
$ go build ./cmd/history
$ ./histquery index # to run inside git repo
```
 Local embeddings (Ollama), SQLite, chromem-go, no cloud, no accounts ask natural-language questions and get cited answers from your own commits.

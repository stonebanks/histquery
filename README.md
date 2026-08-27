# histquery
Self-hosted CLI that turns git commit history into a searchable memory.

## dependencies 

### Ollama
Runs an embedding model locally. By default histquery is using `nomic-embed-text`. It's light and runs well on the CPU.

```bash
$ ollama pull nomic-embed-text # pull embedding model
```

## build

```bash
$ go build ./cmd/history
$ ./histquery index # to run inside git repo
```
 Local embeddings (Ollama), SQLite, no cloud, no accounts ask natural-language questions and get cited answers from your own commits.

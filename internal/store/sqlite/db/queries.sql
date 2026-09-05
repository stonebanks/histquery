-- name: InsertCommit :exec
INSERT INTO commits (
    sha, author_name, author_email, author_date,
    committer_name, committer_email, committer_date, message
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT (sha) DO NOTHING;

-- name: InsertEmbedding :exec
INSERT INTO embeddings (
    commit_sha, source, model, dim, vector
) VALUES (
    ?, ?, ?, ?, ?
)
ON CONFLICT (commit_sha, model, source) DO NOTHING;

-- name: ListUnsyncedEmbeddings :many
SELECT commit_sha, source, model, dim, vector
FROM embeddings
WHERE synced_to_chromem_at IS NULL;

-- name: MarkEmbeddingSynced :exec
UPDATE embeddings SET synced_to_chromem_at = ?
WHERE commit_sha = ? AND source = ? and model = ?;

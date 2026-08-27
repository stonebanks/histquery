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

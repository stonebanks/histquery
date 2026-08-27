-- +goose Up
CREATE TABLE commits (
    sha             TEXT PRIMARY KEY,
    author_name     TEXT NOT NULL,
    author_email    TEXT,
    author_date     DATETIME,
    committer_name  TEXT NOT NULL,
    committer_email TEXT,
    committer_date  DATETIME,
    message         TEXT NOT NULL
);

CREATE INDEX idx_author ON commits(author_name);

CREATE TABLE embeddings (
    id         INTEGER PRIMARY KEY,
    commit_sha TEXT NOT NULL REFERENCES commits(sha) ON DELETE CASCADE,
    source     TEXT NOT NULL,   -- 'message' or 'diff_summary'
    model      TEXT NOT NULL,
    dim        INTEGER NOT NULL,
    vector     BLOB NOT NULL,   -- little-endian float32s, dim*4 bytes
    UNIQUE (commit_sha, model, source),
    CHECK (length(vector) = dim * 4)
);

-- +goose Up
CREATE TABLE commits (
    sha TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_email TEXT,
    message TEXT NOT NULL,
    committed_at DATETIME
);

CREATE INDEX idx_author ON commits(author_name);
CREATE INDEX idx_repo ON commits(repo);

CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    commit_sha TEXT REFERENCES commits(sha),
    source TEXT NOT NULL,   -- 'message' or 'diff_summary'
    model TEXT NOT NULL,
    vector BLOB NOT NULL
);

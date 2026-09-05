-- +goose Up

ALTER TABLE embeddings ADD COLUMN synced_to_chromem_at DATETIME;
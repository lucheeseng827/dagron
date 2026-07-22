-- Environments (Postgres). Mirrors migrations/021_environments.sql — see that
-- file for the design notes. Kept IF NOT EXISTS-idempotent because dagron-api
-- also ensures these tables at startup (it may boot before the engine's
-- migrations have run, same pattern as the users table).

CREATE TABLE IF NOT EXISTS environments (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    variables   TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS environment_secrets (
    environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    ciphertext     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (environment_id, name)
);

ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS environment TEXT;

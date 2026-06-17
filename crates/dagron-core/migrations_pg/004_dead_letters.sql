-- Workflow scheduler v4 — dead-letter store (Postgres).
--
-- Mirrors migrations/004_dead_letters.sql. A poison submission that fails to
-- parse, or whose create_run keeps failing, is parked here by the ingest actor
-- and then acked off the broker so it can't nack-loop forever. `failures` is
-- BIGINT to match the i64 binding code.

CREATE TABLE IF NOT EXISTS dead_letters (
    id            TEXT PRIMARY KEY NOT NULL,
    payload       TEXT NOT NULL,
    error         TEXT NOT NULL,
    source        TEXT NOT NULL,
    failures      BIGINT NOT NULL DEFAULT 1,
    first_seen_at TEXT NOT NULL,
    last_error_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_seen ON dead_letters(first_seen_at);
-- Operators filter by source when tracing which broker is producing poison.
CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letters(source);

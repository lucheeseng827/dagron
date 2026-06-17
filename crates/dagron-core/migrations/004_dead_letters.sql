-- Workflow scheduler v4 — dead-letter store (SQLite).
--
-- A poison submission — one that fails to parse into a DAG, or whose create_run
-- keeps failing — must not nack-loop forever in the queue. After the ingest
-- actor gives up it parks the raw payload here, then acks the source so the
-- message leaves the broker. The row is the durable record of *why* a submission
-- never became a run; an operator can inspect, redrive, or discard it.

CREATE TABLE IF NOT EXISTS dead_letters (
    id            TEXT PRIMARY KEY NOT NULL,
    payload       TEXT NOT NULL,        -- the raw submission, verbatim
    error         TEXT NOT NULL,        -- last failure reason
    source        TEXT NOT NULL,        -- which WorkflowSource produced it
    failures      INTEGER NOT NULL DEFAULT 1,
    first_seen_at TEXT NOT NULL,
    last_error_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_seen ON dead_letters(first_seen_at);
-- Operators filter by source when tracing which broker is producing poison.
CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letters(source);

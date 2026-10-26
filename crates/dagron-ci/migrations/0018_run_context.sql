-- Mirrors migrations_pg/0018_run_context.sql. The comments there say what
-- the column and the tables are for.
ALTER TABLE ci_events ADD COLUMN head_ref TEXT;

CREATE TABLE ci_run_numbers (
    repo_id  TEXT NOT NULL,
    workflow TEXT NOT NULL,
    last     INTEGER NOT NULL,
    PRIMARY KEY (repo_id, workflow)
);

CREATE TABLE ci_run_numbered (
    idempotency_key TEXT PRIMARY KEY,
    run_id          INTEGER NOT NULL,
    run_number      INTEGER NOT NULL
);

-- Mirrors migrations_pg/0031_status_contexts.sql: who writes under a
-- commit's status context. The comments there say what each column is for.
CREATE TABLE ci_contexts (
    repo_id      TEXT NOT NULL,
    sha          TEXT NOT NULL,
    context      TEXT NOT NULL,
    run_id       TEXT,
    lease_holder TEXT,
    lease_until  TEXT,
    written_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (repo_id, sha, context)
);
CREATE INDEX ci_contexts_written ON ci_contexts (written_at);

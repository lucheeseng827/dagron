-- Mirrors migrations_pg/0003_poll.sql: the poll sweep's schedule and what it
-- last saw. Timestamps are RFC 3339 text; the comments there say what each
-- column is for.
ALTER TABLE ci_repos ADD COLUMN poll_secs   INTEGER;
ALTER TABLE ci_repos ADD COLUMN poll_due_at TEXT;
ALTER TABLE ci_repos ADD COLUMN poll_claim  TEXT;
ALTER TABLE ci_repos ADD COLUMN polled_at   TEXT;
ALTER TABLE ci_repos ADD COLUMN poll_etag   TEXT;
ALTER TABLE ci_repos ADD COLUMN poll_error  TEXT;
CREATE INDEX ci_repos_poll_due ON ci_repos (poll_due_at);

CREATE TABLE ci_poll_refs (
    repo_id TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    ref     TEXT NOT NULL,
    sha     TEXT NOT NULL,
    PRIMARY KEY (repo_id, ref)
);

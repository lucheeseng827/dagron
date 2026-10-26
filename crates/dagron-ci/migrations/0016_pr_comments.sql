-- Mirrors migrations_pg/0016_pr_comments.sql: a pull request's summary
-- comment, and what it is written from. The comments there say what each
-- column is for.
CREATE TABLE ci_pr_comments (
    repo_id    TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    pr         INTEGER NOT NULL,
    comment_id TEXT,
    said       TEXT,
    due_at     TEXT,
    written_at TEXT,
    claim      TEXT,
    again      INTEGER NOT NULL DEFAULT 0,
    error      TEXT,
    PRIMARY KEY (repo_id, pr)
);
CREATE INDEX ci_pr_comments_due ON ci_pr_comments (due_at) WHERE due_at IS NOT NULL;

ALTER TABLE ci_runs ADD COLUMN jobs TEXT;

CREATE INDEX ci_events_pr ON ci_events (repo_id, pr) WHERE pr IS NOT NULL;

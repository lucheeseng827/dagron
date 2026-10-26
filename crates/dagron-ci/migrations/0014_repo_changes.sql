-- Mirrors migrations_pg/0014_repo_changes.sql: a repository that is
-- disconnected keeps its row, and each change to one is recorded with who
-- made it. The comments there say what each is for.
ALTER TABLE ci_repos ADD COLUMN disconnected_at TEXT;

CREATE TABLE ci_repo_changes (
    id      TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL
            CHECK (action IN ('connected', 'reconnected', 'changed', 'disconnected')),
    detail  TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX ci_repo_changes_repo ON ci_repo_changes (repo_id, at);

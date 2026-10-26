-- Mirrors migrations_pg/0033_flakes.sql: the jobs that flake, and the issue
-- filed of one that keeps doing so. The comments there say what each
-- column is for.
CREATE TABLE ci_flakes (
    run_id   TEXT NOT NULL,
    job      TEXT NOT NULL,
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    pipeline TEXT NOT NULL,
    cell     TEXT NOT NULL,
    sha      TEXT NOT NULL,
    failed   INTEGER NOT NULL,
    seen_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (run_id, job)
);
CREATE INDEX ci_flakes_job ON ci_flakes (repo_id, pipeline, job, seen_at);

CREATE TABLE ci_flake_issues (
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    pipeline TEXT NOT NULL,
    job      TEXT NOT NULL,
    issue    TEXT,
    said     TEXT,
    since    TEXT,
    due_at   TEXT,
    claim    TEXT,
    again    INTEGER NOT NULL DEFAULT 0,
    error    TEXT,
    PRIMARY KEY (repo_id, pipeline, job)
);
CREATE INDEX ci_flake_issues_due ON ci_flake_issues (due_at) WHERE due_at IS NOT NULL;

-- Mirrors migrations_pg/0015_fleet.sql: fleet mode's jobs, as GitHub says
-- they stand, the runners the fleet started for them, and when a
-- repository's fleet is next looked over. The comments there say what each
-- is for.
CREATE TABLE ci_fleet_jobs (
    forge_host   TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    repo_id      TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    workflow_run TEXT NOT NULL,
    run_attempt  INTEGER NOT NULL DEFAULT 1,
    name         TEXT NOT NULL DEFAULT '',
    workflow     TEXT,
    labels       TEXT NOT NULL,
    class        TEXT NOT NULL,
    head_sha     TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('queued', 'running', 'done')),
    conclusion   TEXT,
    runner_name  TEXT,
    queued_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at   TEXT,
    finished_at  TEXT,
    PRIMARY KEY (forge_host, job_id)
);
CREATE INDEX ci_fleet_jobs_waiting ON ci_fleet_jobs (repo_id, class) WHERE state = 'queued';

CREATE TABLE ci_fleet_runners (
    name        TEXT PRIMARY KEY,
    repo_id     TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    class       TEXT NOT NULL,
    cell        TEXT NOT NULL,
    forge_id    TEXT,
    run_id      TEXT,
    state       TEXT NOT NULL CHECK (state IN ('wanted', 'launched', 'busy', 'ended')),
    job_id      TEXT,
    reason      TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    launched_at TEXT,
    ended_at    TEXT,
    swept_at    TEXT
);
CREATE INDEX ci_fleet_runners_open ON ci_fleet_runners (repo_id) WHERE swept_at IS NULL;

ALTER TABLE ci_repos ADD COLUMN fleet_due_at TEXT;
ALTER TABLE ci_repos ADD COLUMN fleet_claim TEXT;

-- Fleet mode: GitHub Actions stays the coordinator of a repository's
-- workflows, and a job of its that asks for one of the operator's labels is
-- run by GitHub's own runner, which dagron-ci starts as a dagron run.

-- A job, as GitHub says it stands. Kept for each job that asks for a label
-- the fleet serves, whichever runner takes it: a runner is registered for a
-- label, and GitHub gives it any job that asks for that label.
CREATE TABLE ci_fleet_jobs (
    forge_host   TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    repo_id      TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    -- The workflow run the job is of. A workflow run is what is counted as
    -- one run, whatever its jobs.
    workflow_run TEXT NOT NULL,
    run_attempt  INTEGER NOT NULL DEFAULT 1,
    name         TEXT NOT NULL DEFAULT '',
    workflow     TEXT,
    -- A JSON array, as the job's runs-on asked.
    labels       TEXT NOT NULL,
    -- The fleet's label that serves it.
    class        TEXT NOT NULL,
    head_sha     TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('queued', 'running', 'done')),
    conclusion   TEXT,
    -- The runner that took it, the fleet's or another's.
    runner_name  TEXT,
    queued_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    PRIMARY KEY (forge_host, job_id)
);
CREATE INDEX ci_fleet_jobs_waiting ON ci_fleet_jobs (repo_id, class) WHERE state = 'queued';

-- A runner the fleet started: one registration with the forge, one dagron
-- run that hosts it, and one job, which it takes before it exits.
--   wanted    its row is written, and nothing else is yet
--   launched  it is registered, and its run is submitted
--   busy      GitHub says it has a job
--   ended     its job is over, or it was given up, and `reason` says which
-- `swept_at` is set once what it left is taken back: its registration with
-- the forge, and the secret its run was given.
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
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    launched_at TIMESTAMPTZ,
    ended_at    TIMESTAMPTZ,
    swept_at    TIMESTAMPTZ
);
CREATE INDEX ci_fleet_runners_open ON ci_fleet_runners (repo_id) WHERE swept_at IS NULL;

-- When a repository's fleet is next looked over, and by whom now. NULL: it
-- has no job waiting and no runner out, and nothing looks.
ALTER TABLE ci_repos ADD COLUMN fleet_due_at TIMESTAMPTZ;
ALTER TABLE ci_repos ADD COLUMN fleet_claim TEXT;

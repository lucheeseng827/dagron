-- A pull request's head branch, as the forge names it (`feature/x`): a
-- workflow reads it as `github.head_ref`. NULL for every other event.
ALTER TABLE ci_events ADD COLUMN head_ref TEXT;

-- The counts a workflow's runs are numbered from: `github.run_id`, a count
-- of every run of the repository's (workflow ''), and `github.run_number`,
-- a count of each workflow file's runs on it. Each is the last number given.
CREATE TABLE ci_run_numbers (
    repo_id  TEXT NOT NULL,
    workflow TEXT NOT NULL,
    last     BIGINT NOT NULL,
    PRIMARY KEY (repo_id, workflow)
);

-- The numbers each run was given, by the run's idempotency key: an event
-- handled again reads its run's numbers back, so the run it submits again
-- is the one it submitted.
CREATE TABLE ci_run_numbered (
    idempotency_key TEXT PRIMARY KEY,
    run_id          BIGINT NOT NULL,
    run_number      BIGINT NOT NULL
);

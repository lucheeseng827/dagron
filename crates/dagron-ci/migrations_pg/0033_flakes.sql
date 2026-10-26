-- A job that flakes (CI_FLAKE_ISSUES): one of a trusted run that
-- passed only after an attempt of it failed, and the issue filed of a job
-- that keeps doing so.
--
-- `ci_flakes`: each such job, once a run: kept for CI_FLAKE_DAYS, which is
-- how far back a job's flakes are counted. A run's job is one row however
-- often its end is looked at.
CREATE TABLE ci_flakes (
    run_id   TEXT NOT NULL,
    job      TEXT NOT NULL,
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    -- What `ci_runs` says the run ran: a pipeline's path, or `workflow:ID`.
    pipeline TEXT NOT NULL,
    cell     TEXT NOT NULL,
    sha      TEXT NOT NULL,
    -- How many of its attempts failed before the one that passed.
    failed   BIGINT NOT NULL,
    seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, job)
);
CREATE INDEX ci_flakes_job ON ci_flakes (repo_id, pipeline, job, seen_at);

-- `ci_flake_issues`: the issue of one job of one pipeline, once it has
-- flaked CI_FLAKE_ISSUES times. It has one writer at a time, as a pull
-- request's comment has (`ci_pr_comments`): a writer claims the row (`claim`,
-- with `due_at` as the end of its lease), and a flake meanwhile sets
-- `again`, so that the row is due at once after.
CREATE TABLE ci_flake_issues (
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    pipeline TEXT NOT NULL,
    job      TEXT NOT NULL,
    -- The forge's number for the issue, while one is open.
    issue    TEXT,
    -- A digest of what it says: a write that would say the same is not made.
    said     TEXT,
    -- The flakes before this are not counted: an issue a person closed was
    -- of them.
    since    TIMESTAMPTZ,
    due_at   TIMESTAMPTZ,
    claim    TEXT,
    again    BOOLEAN NOT NULL DEFAULT FALSE,
    -- Why it was last not written.
    error    TEXT,
    PRIMARY KEY (repo_id, pipeline, job)
);
CREATE INDEX ci_flake_issues_due ON ci_flake_issues (due_at) WHERE due_at IS NOT NULL;

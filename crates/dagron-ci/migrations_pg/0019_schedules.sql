-- The scheduler: a workflow file's `on.schedule`, read at the head of the
-- repository's default branch, queues a `schedule` event as each of its
-- crons fires. Each repository is looked at in its turn, as the poll sweep
-- looks at it.
-- When the repository is next due. NULL is due now.
ALTER TABLE ci_repos ADD COLUMN schedule_due_at TIMESTAMPTZ;
-- The replica looking at it, until it is done or `schedule_due_at` passes;
-- one that is done after another replica took the repository over writes
-- nothing.
ALTER TABLE ci_repos ADD COLUMN schedule_claim TEXT;
-- The commit the repository's schedules were last read at: they are read
-- again once the default branch has moved.
ALTER TABLE ci_repos ADD COLUMN schedule_sha TEXT;
-- Why the last look failed; NULL once one succeeds.
ALTER TABLE ci_repos ADD COLUMN schedule_error TEXT;
CREATE INDEX ci_repos_schedule_due ON ci_repos (schedule_due_at);

-- Each cron of each workflow file, and when it next fires and last fired,
-- in seconds since the epoch. `next_at` NULL never fires.
CREATE TABLE ci_schedules (
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    workflow TEXT NOT NULL,
    cron     TEXT NOT NULL,
    next_at  BIGINT,
    last_at  BIGINT,
    PRIMARY KEY (repo_id, workflow, cron)
);

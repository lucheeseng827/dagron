-- Mirrors migrations/051_task_quota_wait.sql (SQLite). See there for the full
-- reasoning.
--
-- Quota back-pressure on the task record: a pod refused because the Kubernetes
-- namespace is at its ResourceQuota goes back to 'ready' with a future
-- scheduled_at and its attempt given back, and these columns are what that
-- wait has to remember. They carry it across a restart or a hand-over to
-- another replica for a task at rest in the queue; a task whose probe was in
-- flight when its engine stopped starts a new wait when its lease is recovered.
--
-- A row is waiting on quota iff
--
--     status = 'ready' AND quota_wait_version = version
--
-- and the columns are never cleared: every claim and every operator reset
-- bumps `version`, so the marker stops matching by itself. A reader applies
-- that rule; it does not test quota_wait_since for NULL.
--
-- No index, so no CONCURRENTLY split: both readers filter on status = 'ready',
-- which idx_task_runs_status already serves. Each ADD COLUMN is catalog-only
-- (the one default is a constant), so none of them rewrites task_runs.

-- RFC-3339 time of the first refusal of the current episode: the clock the
-- bound (DAGRON_QUOTA_WAIT_MAX_SECS) is measured from.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS quota_wait_since   TEXT;
-- The row's `version` when it was handed back. Equal to `version` while the
-- task waits; one behind the fence of the next claim, which is how a second
-- refusal knows it continues this episode rather than starting a new one.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS quota_wait_version BIGINT;
-- Refusals in the current episode.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS quota_waits        BIGINT NOT NULL DEFAULT 0;
-- The last refusal sentence: which quota, and what was requested, used and
-- limited. Capped by the writer (quota_wait::DETAIL_MAX_CHARS).
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS quota_wait_detail  TEXT;

-- Quota back-pressure on the task record (mirrors migrations_pg/066_task_quota_wait.sql).
--
-- A pod the Kubernetes apiserver refuses because the namespace is at its
-- ResourceQuota never started. Counting that as a failed attempt spends the
-- task's retries on a condition that clears by itself, so the engine hands the
-- claim back instead: the row returns to 'ready' with a future scheduled_at and
-- the attempt the claim consumed is given back. These columns are what that
-- wait has to remember.
--
-- On the row, not in the scheduler's memory, so that the bound on the wait
-- survives a restart and a hand-over to another replica for a task at rest in
-- the queue. A task whose probe was in flight when its engine stopped is the
-- exception: lease recovery returns it with `version` already past the marker,
-- so its next refusal starts a new wait, and, as with any lease recovery, that
-- dispatch's attempt is not given back.
--
-- A row is waiting on quota iff
--
--     status = 'ready' AND quota_wait_version = version
--
-- and the columns are never cleared. Every claim bumps `version`, so the marker
-- stops matching by itself the moment the task is dispatched again, and every
-- operator reset (retry, clear, rerun) bumps it too. That is why no reset path
-- has to know these columns exist — and why a reader must apply the rule above
-- rather than test quota_wait_since for NULL: a task that waited an hour ago
-- and has been running ever since still carries the values.
--
-- No index. Both readers filter on status = 'ready', which idx_task_runs_status
-- already serves.

-- RFC-3339 time of the first refusal of the current episode: the clock the
-- bound (DAGRON_QUOTA_WAIT_MAX_SECS) is measured from.
ALTER TABLE task_runs ADD COLUMN quota_wait_since TEXT;
-- The row's `version` when it was handed back. Equal to `version` while the
-- task waits; one behind the fence of the next claim, which is how a second
-- refusal knows it continues this episode rather than starting a new one.
ALTER TABLE task_runs ADD COLUMN quota_wait_version INTEGER;
-- Refusals in the current episode.
ALTER TABLE task_runs ADD COLUMN quota_waits INTEGER NOT NULL DEFAULT 0;
-- The last refusal sentence: which quota, and what was requested, used and
-- limited. Capped by the writer (quota_wait::DETAIL_MAX_CHARS).
ALTER TABLE task_runs ADD COLUMN quota_wait_detail TEXT;

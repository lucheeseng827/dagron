-- Mirrors migrations/042_task_external_job.sql (SQLite). See there for the
-- full reasoning; the index is split into 054 as CREATE INDEX CONCURRENTLY,
-- the same split as 031/032 and 022/023 (a plain CREATE INDEX here takes a
-- lock that blocks writers on a populated task_runs while migrations run at
-- engine connect).
--
-- Deferred external jobs (`defer:`) — the fifth park shape. A task hands work
-- to a system dagron does not own and parks: claim dropped, lease NULLed,
-- `status` still 'running', the remote job's identity on the row, a reconcile
-- sweep polling it. The NULL lease is the crash-recovery argument —
-- `recover_expired_leases` filters `lease_expires_at IS NOT NULL`, so a parked
-- row is provably outside the set it can reclaim.

-- Which poller resolves this row ('spark-k8s', 'http', 'sql', or a registered
-- ExternalPoller kind). NULL = not deferred.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_kind     TEXT;
-- The remote job's identity, opaque to dagron. Its presence IS the park.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_handle   TEXT;
-- Where to reach it, pinned at submit so a later poll cannot be redirected by
-- a workflow edit. Never carries a credential.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_endpoint TEXT;
-- Submission generation: the remote job is named `dagron-<id>-<epoch>`, which
-- is what makes a post-crash resubmit adopt rather than duplicate. `attempt`
-- cannot serve — it increments on every claim including lease recovery, so a
-- name derived from it changes exactly when adoption is needed.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_epoch    BIGINT NOT NULL DEFAULT 0;
-- Park time + `defer.max_wait_secs`; NULL when the author named no ceiling. A
-- column rather than reusing `wake_at` (the time sensor's park reason) so a row
-- never carries two park reasons at once.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS external_deadline_at TEXT;

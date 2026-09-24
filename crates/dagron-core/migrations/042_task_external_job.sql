-- Deferred external jobs (`defer:`) — the fifth park shape (mirrors
-- migrations_pg/053_task_external_job.sql + 054 for its index).
--
-- A task that hands work to a system dagron does not own — a SparkApplication
-- on someone's cluster, a Databricks run, a long ClickHouse statement — should
-- not hold a worker slot for the hours that work takes. `defer:` parks the row
-- instead: claim dropped, lease NULLed, `status` still 'running', and the
-- remote job's identity on the row. A reconcile sweep polls it.
--
-- The whole crash-recovery argument rests on the NULL lease.
-- `recover_expired_leases` filters `lease_expires_at IS NOT NULL`, so a parked
-- row is provably outside the set it can reclaim: kill every scheduler
-- mid-job and the row survives untouched, and any replica's next sweep
-- resumes the poll. Nothing resubmits, because nothing re-ran.
--
-- Columns on task_runs rather than a side table, for the same reason the four
-- existing park shapes (wake_at / wait_url / wait_dataset / sub_run_id) are
-- columns: exactly one park per row, read with the row, never joined.

-- Which poller resolves this row: 'spark-k8s', 'http', 'sql', or a kind a
-- registered `ExternalPoller` claims. NULL = not deferred. Carried separately
-- from the handle so the sweep can route without parsing an opaque string.
ALTER TABLE task_runs ADD COLUMN external_kind TEXT;
-- The remote job's identity, opaque to dagron: a CR name, a Databricks run id,
-- a ClickHouse query_id. Its presence IS the park — `external_handle IS NOT
-- NULL` is the guard every sweep and every resolve matches on, exactly as
-- `sub_run_id IS NOT NULL` is for a parked sub-workflow trigger.
ALTER TABLE task_runs ADD COLUMN external_handle TEXT;
-- Where to reach it, when the handle alone does not say (a Databricks
-- workspace host, a Spark operator namespace). Resolved at submit and pinned
-- here, so a poll after an operator edits the workflow still talks to the
-- endpoint the job actually went to. Never carries a credential.
ALTER TABLE task_runs ADD COLUMN external_endpoint TEXT;
-- Submission generation for this row. The remote job is named
-- `dagron-<task_runs.id>-<external_epoch>`, which is what makes a resubmit
-- after a crash idempotent: the id is stable across lease recovery, so the
-- retried submit reuses the name, the remote system answers AlreadyExists, and
-- the step adopts the running job instead of starting a second one.
--
-- `attempt` cannot serve this purpose: it increments on EVERY claim
-- (including lease recovery), so a name derived from it changes exactly when
-- adoption is needed and the AlreadyExists path would never fire.
--
-- Bumped only where a row that has already submitted is deliberately re-armed
-- for a FRESH job — fail_external, rerun_from_failed, clear_task_with_downstream
-- — so a genuine retry gets a new name rather than adopting the corpse of the
-- attempt that just failed.
ALTER TABLE task_runs ADD COLUMN external_epoch INTEGER NOT NULL DEFAULT 0;
-- When this remote job stops being worth waiting for: park time +
-- `defer.max_wait_secs`, or NULL when the author named no ceiling (then the
-- run's own `run_timeout_secs` is the only bound).
--
-- A column rather than reusing `wake_at`: that is the time sensor's park
-- reason, and a deferred row carrying one would be swept by the wait-sensor
-- reconcile and counted out of POOL_RUNNING_COUNT twice over. Exactly one park
-- reason per row is the invariant the four existing shapes keep.
ALTER TABLE task_runs ADD COLUMN external_deadline_at TEXT;

-- The sweep reads `WHERE external_handle IS NOT NULL AND next_poll_at <= now`
-- ORDER BY next_poll_at. Partial, so it indexes only parked rows — the point
-- of the feature is that a few hundred parked jobs cost rows rather than
-- worker slots, and at that count an unindexed scan twice a second (the
-- default SWEEP_INTERVAL_MS) is the regression the latency gate exists to
-- catch. `due_url_waits` gets away without one only because nobody has more
-- than a handful of HTTP sensors.
CREATE INDEX IF NOT EXISTS idx_task_runs_external
    ON task_runs (next_poll_at)
    WHERE external_handle IS NOT NULL;

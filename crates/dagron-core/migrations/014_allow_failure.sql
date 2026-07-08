-- Per-task allow_failure (fast-win #11): when 1, this task failing does not
-- mark the run failed (an optional / best-effort step). The task still records
-- `failed` and still skips its `all_success` dependents; only the run-status
-- determination in reap_completed_runs ignores it.
ALTER TABLE task_runs ADD COLUMN allow_failure INTEGER NOT NULL DEFAULT 0;

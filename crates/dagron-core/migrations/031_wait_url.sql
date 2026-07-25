-- HTTP wait sensor (spec fast-win #27 follow-on — Airflow HttpSensor). A
-- `type: wait` task with a `wait.url` parks like the time sensor (#27): it stays
-- `running` with `claimed_by`/`lease_expires_at` NULL so the claim scan skips it
-- and lease recovery leaves it alone — no new status value needed. Instead of a
-- deadline the engine polls `wait_url` on a fixed interval (`WAIT_POLL_SECS`,
-- default 15 s) and succeeds the task on the first 2xx. `next_poll_at` throttles
-- the sweep so each parked sensor is GETed at most once per interval. Bounded by
-- the run's own `run_timeout_secs`. NULL `wait_url` = a time sensor or an
-- ordinary task.
ALTER TABLE task_runs ADD COLUMN wait_url TEXT;
ALTER TABLE task_runs ADD COLUMN next_poll_at TEXT;

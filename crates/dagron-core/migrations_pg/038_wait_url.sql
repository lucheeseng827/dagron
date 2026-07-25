-- HTTP wait sensor (#27 follow-on — Airflow HttpSensor). A `type: wait` task
-- with a `wait.url` parks (running, NULL lease) like the time sensor (#27) but
-- carries no `wake_at`; the engine polls `wait_url` every `WAIT_POLL_SECS`
-- (default 15 s) and succeeds it on the first 2xx. `next_poll_at` throttles the
-- sweep. Bounded by the run's `run_timeout_secs`. Mirrors
-- migrations/031_wait_url.sql. NULL `wait_url` = a time sensor or ordinary task.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS wait_url TEXT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS next_poll_at TEXT;

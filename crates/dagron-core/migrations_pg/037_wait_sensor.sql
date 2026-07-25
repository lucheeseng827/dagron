-- Deferrable wait sensor (#27 — Airflow deferrable time sensors / Argo
-- suspend-with-duration). A `type: wait` task parks (running, NULL lease) with
-- its resume deadline in `wake_at` and no worker held; the reconcile wait sweep
-- resolves it once `wake_at <= now`. Mirrors migrations/030_wait_sensor.sql.
-- NULL = an ordinary task.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS wake_at TEXT;

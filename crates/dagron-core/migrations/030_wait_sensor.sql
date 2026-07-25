-- Deferrable wait sensor (spec fast-win #27 — Airflow deferrable time sensors /
-- Argo suspend-with-duration). A `type: wait` task parks with no worker held
-- until `wake_at`, then succeeds. Like the sub-workflow trigger (#23) it stays
-- `running` with `claimed_by`/`lease_expires_at` NULL — the claim scan skips it
-- and lease recovery leaves it alone — so no new status value is needed. The
-- reconcile loop's wait sweep resolves rows where
-- `status = 'running' AND wake_at IS NOT NULL AND wake_at <= now`. NULL = an
-- ordinary task.
ALTER TABLE task_runs ADD COLUMN wake_at TEXT;

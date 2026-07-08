-- Human approval gates (fast-win #19). Mirrors migrations/018_approval.sql.
-- Postgres can ALTER a CHECK constraint in place, so no table rebuild is needed.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS is_approval INTEGER NOT NULL DEFAULT 0;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS approval_timeout_secs BIGINT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS approval_on_timeout TEXT;

-- Widen the status CHECK to admit 'awaiting_approval'. The inline CHECK from
-- migration 001 is auto-named `task_runs_status_check`.
ALTER TABLE task_runs DROP CONSTRAINT IF EXISTS task_runs_status_check;
ALTER TABLE task_runs ADD CONSTRAINT task_runs_status_check
    CHECK (status IN ('pending','ready','running','awaiting_approval',
                      'succeeded','failed','skipped','cancelled'));

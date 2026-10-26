-- A rerun, a retry and a clear keep what they replace: the attempt each one
-- wipes goes to `task_attempts` first. Mirrors
-- migrations/056_replaced_attempts.sql (SQLite), which says why, and what
-- `attempt_base` counts. Each is a column with a constant default or none, so
-- none of them rewrites its table.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS attempt_base BIGINT NOT NULL DEFAULT 0;
ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE task_attempts ADD COLUMN IF NOT EXISTS scheduled_at TEXT;

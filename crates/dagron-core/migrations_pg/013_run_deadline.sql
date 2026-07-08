-- Run-level wall-clock deadline (spec `run_timeout_secs`).
-- Mirrors migrations/010_run_deadline.sql (SQLite): RFC-3339 text timestamp,
-- NULL = no run-level deadline. The engine's deadline sweep fails overdue
-- running runs and cancels their remaining tasks.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS deadline_at TEXT;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_deadline
    ON workflow_runs(deadline_at)
    WHERE deadline_at IS NOT NULL AND status = 'running';

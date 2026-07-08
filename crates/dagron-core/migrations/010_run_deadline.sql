-- Run-level wall-clock deadline (spec `run_timeout_secs`).
-- Set at create_run (created_at + run_timeout_secs, RFC-3339 text like every
-- other timestamp); NULL = no run-level deadline. The engine's deadline sweep
-- fails overdue running runs and cancels their remaining tasks.
ALTER TABLE workflow_runs ADD COLUMN deadline_at TEXT;

-- The sweep polls "running AND deadline_at < now" every tick; keep it indexed.
CREATE INDEX IF NOT EXISTS idx_workflow_runs_deadline
    ON workflow_runs(deadline_at)
    WHERE deadline_at IS NOT NULL AND status = 'running';

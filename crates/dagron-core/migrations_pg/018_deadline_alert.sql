-- Soft SLA deadline alert (fast-win #20). Mirrors migrations/015_deadline_alert.sql.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS alert_deadline_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS alert_fired_at TEXT;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_alert_deadline
    ON workflow_runs(alert_deadline_at)
    WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running';

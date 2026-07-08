-- Soft SLA deadline alert (fast-win #20).
-- `alert_deadline_at` is set at create_run from the spec's `deadline`; the
-- engine's alert sweep emits a `run.deadline_exceeded` outbox event once when a
-- still-running run passes it, stamping `alert_fired_at` for fire-once
-- idempotency. Unlike `run_timeout_secs`, the run is NOT cancelled.
ALTER TABLE workflow_runs ADD COLUMN alert_deadline_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN alert_fired_at TEXT;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_alert_deadline
    ON workflow_runs(alert_deadline_at)
    WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running';

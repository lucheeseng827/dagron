-- Cron `when` gate + `stopStrategy` (spec fast-win #7).
-- Mirrors migrations/012_schedule_gates.sql (SQLite). See it for column notes.
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS when_expr TEXT;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS stop_expr TEXT;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS stopped_at TEXT;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS stop_reason TEXT;

ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS schedule_id TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_schedule
    ON workflow_runs(schedule_id)
    WHERE schedule_id IS NOT NULL;

-- Cron `when` gate + `stopStrategy` (spec fast-win #7).
--
-- `when_expr`: a per-fire conditional evaluated against the scheduled time's
--   calendar fields (hour/day/weekday/…); when it is false the fire is skipped
--   and only `next_fire_at` advances — the condition cron syntax can't express
--   (e.g. "last business day of month").
-- `stop_expr`: evaluated against this schedule's run outcome counts
--   (succeeded/failed/total) before each fire; when true the schedule
--   auto-stops (disabled, with `stopped_at`/`stop_reason` recording why).
ALTER TABLE schedules ADD COLUMN when_expr TEXT;
ALTER TABLE schedules ADD COLUMN stop_expr TEXT;
ALTER TABLE schedules ADD COLUMN stopped_at TEXT;
ALTER TABLE schedules ADD COLUMN stop_reason TEXT;

-- Which schedule created a run — stamped by the DB-schedule loop so `stop_expr`
-- can count this schedule's outcomes. NULL for API/queue/cron-file runs.
ALTER TABLE workflow_runs ADD COLUMN schedule_id TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_schedule
    ON workflow_runs(schedule_id)
    WHERE schedule_id IS NOT NULL;

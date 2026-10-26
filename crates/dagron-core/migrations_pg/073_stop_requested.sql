-- A cancel asks, and does not say the work has stopped: a task a worker holds
-- stays `running` with `stop_requested_at` set until that worker says the work
-- is gone, and its run stays `running` with the status it will take
-- (`stopping_as`) until none of its tasks is still stopping. Mirrors
-- migrations/055_stop_requested.sql (SQLite), which says why. The indexes are
-- 074 and 075, each on its own, so they can be built CONCURRENTLY.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS stop_requested_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS stopping_as TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS stop_requested_at TEXT;

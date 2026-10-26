-- A run whose `notify.git` start (the `pending` commit status) is still to be
-- posted. Mirrors migrations/054_run_start_pending.sql (SQLite), which says
-- why. The index is 070, on its own, so it can be built CONCURRENTLY.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS start_pending_at TEXT;

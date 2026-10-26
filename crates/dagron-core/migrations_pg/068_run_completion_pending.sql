-- A run that ended outside the engine's reaper (a cancel, the run_timeout_secs
-- sweep), whose end-of-run work is owed. Mirrors
-- migrations/053_run_completion_pending.sql (SQLite), which says why. The
-- index is 068, on its own, so it can be built CONCURRENTLY.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS completion_pending_at TEXT;

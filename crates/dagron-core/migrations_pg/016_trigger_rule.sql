-- Per-task trigger rule (spec fast-win #10).
-- Mirrors migrations/013_trigger_rule.sql (SQLite). See it for column notes.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS trigger_rule TEXT NOT NULL DEFAULT 'all_success';

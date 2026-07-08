-- Run result (fast-win #15). Mirrors migrations/016_result_from.sql.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS result_from TEXT;

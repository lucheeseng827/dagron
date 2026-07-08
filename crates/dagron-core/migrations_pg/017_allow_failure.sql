-- Per-task allow_failure (fast-win #11). Mirrors migrations/014_allow_failure.sql.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS allow_failure BIGINT NOT NULL DEFAULT 0;

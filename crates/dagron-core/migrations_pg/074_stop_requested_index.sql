-- no-transaction
-- The teardown claim reads this index on every sweep: the tasks asked to stop,
-- by when their lease lapses. Partial: the column is NULL on all but the few
-- tasks of a run cancelled since their work stopped. Mirrors
-- migrations/055_stop_requested.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 068 and 071, so
-- building it never blocks a claim. An interrupted build can leave an INVALID
-- index that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_stop_requested
    ON task_runs (lease_expires_at) WHERE stop_requested_at IS NOT NULL;

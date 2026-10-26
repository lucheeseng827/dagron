-- no-transaction
-- The sweep that lets waiting runs in reads this index on every pass: the
-- runs that wait for their workflow's cap. Partial: both columns are NULL on
-- every run that never waited, and the second is set once one is let in.
-- And of running runs alone: a run cancelled as it waited, or one another
-- took the place of, keeps the first column and never gets the second, and
-- would stay in the index for as long as its row is kept.
-- Mirrors migrations/060_queued_runs.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 075, so building
-- it never blocks a run's creation. An interrupted build can leave an INVALID
-- index that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_waiting
    ON workflow_runs (id)
    WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running';

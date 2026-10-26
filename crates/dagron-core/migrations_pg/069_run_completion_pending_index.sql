-- no-transaction
-- The claim of runs whose end-of-run work is owed reads this index on every
-- reconcile tick of every replica. Partial: the column is NULL on all but the
-- few runs ended since the last tick. Mirrors
-- migrations/053_run_completion_pending.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 065: workflow_runs
-- can be large, and a plain CREATE INDEX would block run creation while
-- migrations run at engine connect. An interrupted build can leave an INVALID
-- index that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_completion_pending
    ON workflow_runs (completion_pending_at) WHERE completion_pending_at IS NOT NULL;

-- no-transaction
-- The claim of runs whose start is still to be announced reads this index on
-- every reconcile tick of a replica with a forge token. Partial: the column is
-- NULL on all but the few runs created since the last tick. Mirrors
-- migrations/054_run_start_pending.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 068, so building it
-- never blocks run creation. An interrupted build can leave an INVALID index
-- that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_start_pending
    ON workflow_runs (start_pending_at) WHERE start_pending_at IS NOT NULL;

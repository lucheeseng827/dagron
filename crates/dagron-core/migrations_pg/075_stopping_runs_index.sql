-- no-transaction
-- The sweep that ends a run an older engine left with nothing stopping reads
-- this index on every pass: the runs being stopped. Partial: the column is
-- NULL on all but the few runs cancelled since their work stopped. Mirrors
-- migrations/055_stop_requested.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 074, so building
-- it never blocks a run's creation. An interrupted build can leave an INVALID
-- index that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_stopping
    ON workflow_runs (id) WHERE stopping_as IS NOT NULL;

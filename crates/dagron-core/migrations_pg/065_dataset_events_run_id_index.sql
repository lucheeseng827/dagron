-- no-transaction
-- The datasets a run produced, read by run. Mirrors
-- migrations/050_dataset_events_run_id.sql (SQLite), which says why.
--
-- CONCURRENTLY in its own single-statement migration, like 047 and 048: the
-- ledger is append-only and can be large, and a plain CREATE INDEX would block
-- its writers while migrations run at engine connect. An interrupted build can
-- leave an INVALID index that IF NOT EXISTS skips — drop it and re-run
-- migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dataset_events_run_id
    ON dataset_events (run_id) WHERE run_id IS NOT NULL;

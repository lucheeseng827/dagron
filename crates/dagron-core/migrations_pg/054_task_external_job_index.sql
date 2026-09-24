-- no-transaction
-- The parked-external sweep index for 053 (see 023/032 for the same
-- CONCURRENTLY / no-transaction rationale). The sweep reads
-- `WHERE external_handle IS NOT NULL AND next_poll_at <= now` ORDER BY
-- next_poll_at; partial so it indexes only parked rows.
--
-- This one is not optional. The feature's whole claim is that a few hundred
-- parked jobs cost rows rather than worker slots, and at that count an
-- unindexed scan at the default SWEEP_INTERVAL_MS is a regression on the
-- reconcile loop under exactly the load the feature is for.
--
-- CONCURRENTLY so building it on a populated task_runs never blocks the live
-- claim/mark writers of a running engine. If a concurrent build is interrupted
-- it can leave an INVALID index that IF NOT EXISTS then skips — drop the
-- invalid index and re-run migrations to rebuild (standard CONCURRENTLY care).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_external
    ON task_runs (next_poll_at)
    WHERE external_handle IS NOT NULL;

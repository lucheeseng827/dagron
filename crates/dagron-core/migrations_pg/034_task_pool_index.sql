-- no-transaction
-- Running-count-by-pool index for the claim's per-pool budget check (see 028/023
-- for the CONCURRENTLY / no-transaction rationale). CONCURRENTLY so building it
-- on a populated task_runs never blocks the live claim/mark writers. Partial on
-- the in-flight rows so it stays tiny.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_pool_running
    ON task_runs (pool)
    WHERE status = 'running' AND pool IS NOT NULL;

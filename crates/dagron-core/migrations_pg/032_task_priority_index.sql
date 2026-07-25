-- no-transaction
-- Priority-first analog of idx_task_runs_ready_scheduled_at (see 023 for the
-- same CONCURRENTLY / no-transaction rationale). claim_ready now orders by
-- priority DESC, scheduled_at, so this index lets the hot claim scan read rows
-- already in claim order without a sort. CONCURRENTLY so building it on a
-- populated task_runs never blocks the live claim/mark writers of a running
-- engine. If a concurrent build is interrupted it can leave an INVALID index
-- that IF NOT EXISTS then skips — drop the invalid index and re-run migrations
-- to rebuild (standard CONCURRENTLY care).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_ready_priority
    ON task_runs (priority DESC, scheduled_at, id)
    WHERE status = 'ready';

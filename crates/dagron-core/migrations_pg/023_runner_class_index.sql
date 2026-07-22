-- no-transaction
-- Class-scoped analog of idx_task_runs_ready_scheduled_at: a class-restricted
-- scheduler's claim scan stays tiny even when another class's backlog is deep.
-- CONCURRENTLY (which is why this is its own no-transaction, single-statement
-- migration — see 022) so building it on a populated task_runs never blocks
-- the claim/mark writers of a live engine. If a concurrent build is interrupted
-- it can leave an INVALID index that IF NOT EXISTS then skips — drop the
-- invalid index and re-run migrations to rebuild (standard CONCURRENTLY care).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_ready_class
    ON task_runs (runner_class, scheduled_at, id)
    WHERE status = 'ready';

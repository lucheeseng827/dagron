-- no-transaction
-- The index for 050's columns, split out because CONCURRENTLY cannot run
-- inside a transaction block and sqlx wraps a multi-statement migration in one
-- (the 022/023, 031/032, 033/034 convention).
--
-- The fleet read this serves is "what did each fault class cost us this week",
-- which scans classified failures by class over a finished_at range — the
-- query behind any reclaimed-GPU-hours number. Partial on the classified rows
-- only: the unclassified ones are the majority and this index never has to
-- look at them.
--
-- If a concurrent build is interrupted it can leave an INVALID index that
-- IF NOT EXISTS then skips — drop the invalid index and re-run migrations to
-- rebuild (standard CONCURRENTLY care, same as 023).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_fault_class
    ON task_runs (fault_class, finished_at)
    WHERE fault_class IS NOT NULL;

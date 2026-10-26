-- no-transaction
-- The time sensors' sweep (reconcile_waits) reads this index on every pass:
-- the `type: wait` tasks that are parked, by when each is due. Partial, and
-- of running rows alone: the sweep sets a sensor succeeded and keeps its
-- `wake_at`, and only a reset clears it, so every sensor that ever ran would
-- stay in the index for as long as its row is kept.
--
-- `status` leads, though on Postgres every row here is 'running': the SQLite
-- twin has the same text, and SQLite, which keeps no statistics of this
-- table, took idx_task_runs_status for the sweep's read over a partial index
-- led by any other column. Mirrors migrations/076_sweep_indexes.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 101, so building
-- it never blocks a claim. An interrupted build can leave an INVALID index
-- that IF NOT EXISTS skips; the engine warns of one at start, and REINDEX
-- INDEX CONCURRENTLY rebuilds it.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_wake_due
    ON task_runs (status, wake_at)
    WHERE status = 'running' AND wake_at IS NOT NULL;

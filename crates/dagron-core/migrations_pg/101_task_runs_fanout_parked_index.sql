-- no-transaction
-- The fan-out sweep (reconcile_fanouts) reads this index on every pass: the
-- barriers that are parked, to be expanded or to wait for their instances.
-- Partial, and of running rows alone: a barrier keeps `fanout_of` after it
-- has resolved, as every parked row keeps what parked it once it has ended
-- (reconcile_waits keeps `wake_at` on the row it sets succeeded; only a
-- reset clears them), and would stay in the index for as long as its row is
-- kept.
--
-- `status` leads, though on Postgres every row here is 'running': the SQLite
-- twin has the same text, and SQLite, which keeps no statistics of this
-- table, took idx_task_runs_status for the sweep's read over a partial index
-- led by any other column. Mirrors migrations/076_sweep_indexes.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 083, so building
-- it never blocks a claim. An interrupted build can leave an INVALID index
-- that IF NOT EXISTS skips; the engine warns of one at start, and REINDEX
-- INDEX CONCURRENTLY rebuilds it.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_fanout_parked
    ON task_runs (status, claimed_by)
    WHERE status = 'running' AND fanout_of IS NOT NULL AND claimed_by IS NULL;

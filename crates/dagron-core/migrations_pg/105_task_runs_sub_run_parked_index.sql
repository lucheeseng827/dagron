-- no-transaction
-- The sub-workflow sweep (reconcile_subworkflows) reads this index on every
-- pass: the trigger tasks that are parked on a child run. Partial, and of
-- running rows alone: a trigger keeps its `sub_run_id` once it has resolved,
-- as every parked row keeps what parked it after it has ended (only a reset
-- clears them), and would stay in the index for as long as its row is kept.
--
-- `status` leads, though on Postgres every row here is 'running': the SQLite
-- twin has the same text, and SQLite, which keeps no statistics of this
-- table, took idx_task_runs_status for the sweep's read over a partial index
-- led by any other column. Mirrors migrations/076_sweep_indexes.sql (SQLite).
--
-- 041's idx_task_runs_sub_run_id stays. The recursion guard
-- (sub_workflow_depth) looks a run's parent up by `sub_run_id = $1` alone,
-- of a row in any status, which this index cannot answer.
--
-- CONCURRENTLY in its own single-statement migration, like 101, so building
-- it never blocks a claim. An interrupted build can leave an INVALID index
-- that IF NOT EXISTS skips; the engine warns of one at start, and REINDEX
-- INDEX CONCURRENTLY rebuilds it.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_sub_run_parked
    ON task_runs (status, sub_run_id)
    WHERE status = 'running' AND sub_run_id IS NOT NULL;

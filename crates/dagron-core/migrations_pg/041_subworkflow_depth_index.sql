-- no-transaction
-- Reverse index on the sub-workflow parking column (#23). Mirrors
-- migrations/034_subworkflow_depth_index.sql (SQLite).
--
-- The recursion guard walks `sub_run_id` backwards — given a run, `sub_run_id
-- = $1` finds the parked task that spawned it — so the chain depth is derivable
-- without a `parent_run_id` column. Each hop would otherwise be a sequential
-- scan of task_runs.
--
-- Partial (`WHERE sub_run_id IS NOT NULL`): only parked sub-workflow parents
-- carry a value, so the index stays small while also covering the reconcile
-- sweep's forward read. CONCURRENTLY (hence `no-transaction`) so building it on
-- a populated task_runs never blocks a running engine's claim/mark writers —
-- same rationale as 032. An interrupted concurrent build can leave an INVALID
-- index that IF NOT EXISTS then skips; drop it and re-run migrations.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_sub_run_id
    ON task_runs (sub_run_id)
    WHERE sub_run_id IS NOT NULL;

-- Reverse index on the sub-workflow parking column (#23).
--
-- `sub_run_id` has so far only ever been read forward — "which parked parents
-- are waiting on a child" (`status = 'running' AND sub_run_id IS NOT NULL`).
-- The recursion guard reads it *backwards*: given a run, `sub_run_id = ?` finds
-- the parked task that spawned it, and repeating that walks up to the root. It
-- is the only parentage the schema records, so no `parent_run_id` column is
-- needed — but without an index each hop is a full task_runs scan.
--
-- Partial (`WHERE sub_run_id IS NOT NULL`): only parked sub-workflow parents
-- carry a value, a small fraction of task_runs, so the index stays tiny while
-- still covering the reconcile sweep's forward read.
CREATE INDEX IF NOT EXISTS idx_task_runs_sub_run_id
    ON task_runs (sub_run_id)
    WHERE sub_run_id IS NOT NULL;

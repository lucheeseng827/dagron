-- Indexes that match the reconcile loop's sweeps.
-- The second to the sixth are migrations_pg/101 to 105, with the same names
-- and the same text; each of those says which sweep reads it.
--
-- Every one is led by `status`, and that is the rule for an index a sweep
-- reads here. Nothing writes sqlite_stat1 (init_pool runs no ANALYZE), and
-- with no statistics SQLite took idx_task_runs_status for a sweep's
-- `status = 'running' AND ...` over a partial index led by any other column,
-- and so read every running row, and sorted them where the sweep orders.
-- Led by `status`, with the sweep's other terms in its WHERE, the partial
-- index is the one taken. On the SQLite this build bundles (3.46.0) the
-- reads of the lease sweep, the fan-out, time, HTTP, dataset and
-- sub-workflow sweeps, the external polls and the teardown claim each move
-- onto theirs, and the plans of the reads beside them (the claim, a run's
-- parent, a run's running tasks, a pool's count, the external cancels) stay
-- as they were. The plan tests in db/sqlite.rs
-- (sweep_reads_plan_on_their_partial_indexes, plans_that_must_not_move)
-- hold both to that.
--
-- Of running rows alone, as on Postgres: a parked row keeps what parked it
-- after it has ended, and only a reset clears it.
--
-- Two have no Postgres twin here. The lease index is Postgres 001's, led
-- here by `status`, and SQLite never had one: the lease sweep and the
-- teardown claim read it. The external polls' is SQLite's alone: on
-- Postgres, 054's index serves them, and the planner has statistics to take
-- it by.
CREATE INDEX IF NOT EXISTS idx_task_runs_running_lease_expires_at
    ON task_runs (status, lease_expires_at)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_fanout_parked
    ON task_runs (status, claimed_by)
    WHERE status = 'running' AND fanout_of IS NOT NULL AND claimed_by IS NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_wake_due
    ON task_runs (status, wake_at)
    WHERE status = 'running' AND wake_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_wait_url_due
    ON task_runs (status, next_poll_at)
    WHERE status = 'running' AND wait_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_wait_dataset_parked
    ON task_runs (status, wait_dataset)
    WHERE status = 'running' AND wait_dataset IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_sub_run_parked
    ON task_runs (status, sub_run_id)
    WHERE status = 'running' AND sub_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_external_running
    ON task_runs (status, next_poll_at)
    WHERE status = 'running' AND external_handle IS NOT NULL;

-- A run that ended outside the engine's reaper, whose end-of-run work is owed.
--
-- The reaper finalizes a run whose tasks are all terminal: it writes the run's
-- `run.completed` outbox event in the same transaction, then runs the rest in
-- its own process (the run sink, OpenLineage, the `notify.git` status, and
-- `notify.webhook` / `notify.slack`). Two paths end a run without it: a cancel,
-- through the engine's API or dagron-api's (another process), and the
-- `run_timeout_secs` sweep. They used to do neither, so a cancelled run left
-- its commit status "pending" forever and its webhook never fired. They now
-- write the outbox event in their own transaction and stamp this column; the
-- next reconcile tick on any engine replica claims the stamp
-- (`claim_pending_completions`) and runs the same work, once.
--
-- NULL on every other row, existing ones included, so adding the column owes
-- nothing for runs that ended before it. The partial index keeps the claim a
-- probe of a near-empty set.
ALTER TABLE workflow_runs ADD COLUMN completion_pending_at TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_completion_pending
    ON workflow_runs (completion_pending_at) WHERE completion_pending_at IS NOT NULL;

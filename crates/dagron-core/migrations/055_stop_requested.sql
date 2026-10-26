-- A cancel asks. It does not say the work has stopped.
--
-- Until now a cancel, a run past its `run_timeout_secs` and a gang sibling's
-- cancel wrote `cancelled` on a running task in the statement that asked, and
-- a cancel or a deadline ended the run there. The task's container or pod went
-- on for a lease renewal and a grace while every record said it was over, and
-- the run's concurrency key was free for the next run to start beside it.
--
-- `task_runs.stop_requested_at` is set on a task a worker holds, in place of
-- that `cancelled`. The row stays `running`, with its claim, until its worker
-- says the work is gone: then it is `cancelled`, and this is cleared. A worker
-- that cannot say so (it died, or could not see its teardown through) lets the
-- lease lapse, and the task is claimed again for a teardown alone.
--
-- `workflow_runs.stopping_as` is the status such a run takes once none of its
-- tasks is still stopping: `cancelled`, or `failed` for a deadline. Until then
-- the run is `running`, which is what it is. It holds its concurrency key, and
-- nothing reads it as ended. Both run columns are cleared at that end.
--
-- No new status and no CHECK rebuild: a reader that knows nothing of these
-- columns sees a run that is still running, and then one that is cancelled.
--
-- NULL on every existing row. The partial indexes are the sweeps': the few
-- stopping tasks, by when their lease lapses, for the teardown claim; and the
-- few runs being stopped, for the sweep that ends one an older engine left.
ALTER TABLE task_runs ADD COLUMN stop_requested_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN stopping_as TEXT;
ALTER TABLE workflow_runs ADD COLUMN stop_requested_at TEXT;
CREATE INDEX IF NOT EXISTS idx_task_runs_stop_requested
    ON task_runs (lease_expires_at) WHERE stop_requested_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_stopping
    ON workflow_runs (id) WHERE stopping_as IS NOT NULL;

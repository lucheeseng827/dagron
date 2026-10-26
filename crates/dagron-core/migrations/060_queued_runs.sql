-- A run that arrives while its workflow's cap is full may wait for a turn.
--
-- `max_active_runs` refused such a run, and that is still what a workflow
-- that says nothing gets. With `when_full: queue` (or `supersede`) the run is
-- made instead, and waits: `workflow_runs.queued_at` is when it began to, and
-- `admitted_at` when it was let in. A run that waits has the first and not
-- the second. One that never waited has neither.
--
-- A run that waits reads `running`, which is what a run that has started
-- nothing reads already: no new status, and no CHECK rebuild. A reader that
-- knows nothing of these columns sees a run whose tasks are all `pending`.
-- It holds a place in its key, in the order it arrived, and is cancelled as
-- any run is.
--
-- Its tasks are held by a dependency none of them has: each is written with
-- `remaining_deps` one more than its own, so nothing advances it, and letting
-- the run in takes that one away again. No statement that advances, claims or
-- counts a task reads a run's row to know whether it may.
--
-- NULL on every existing row. The partial index is the sweep's that lets
-- runs in: the few that wait, which is none on most installations. It is of
-- running runs alone: a run cancelled as it waited, or one another took the
-- place of, keeps `queued_at` and never gets `admitted_at`, and would stay
-- in the index for as long as its row is kept.
ALTER TABLE workflow_runs ADD COLUMN queued_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN admitted_at TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_waiting
    ON workflow_runs (id)
    WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running';

-- The runs a reconcile loop should look at, for a backend with no
-- LISTEN/NOTIFY. Postgres names a run in a
-- notification sent by the writer; here the file names it, by triggers, so
-- every writer is heard: the engine, dagron-api's own SQL on the same file
-- (a gate's decision, a run submitted with no task), an older binary of
-- either, and the sqlite3 shell. A wake commits or rolls back with the write
-- that made it.
--
-- A run can have ended only if one of these changed: a task's status, a
-- task gone, the run made, or its status, stopping_as, queued_at or
-- admitted_at. Each has a trigger here. The engine takes the rows each tick
-- (db::sqlite::Waker::take_woken) and looks at those runs by their keys; its
-- first take after a start empties the table and looks at every running
-- run, which covers whatever a crash, or an engine from before this, left.
-- One consumer: the engine empties the table. Nothing else may read it as a
-- feed.
--
-- idx_workflow_runs_reapable is the slice's (db::sqlite::reap_slice), the
-- net under a wake that is lost. Led by `status` by 076's rule: its loop
-- uses `status` and `id`, a superset of both the enterprise build's index
-- of (status, created_at) (`status`) and the primary key's (`id`), so it is
-- taken on either build, in either order.
-- The slice carries its WHERE word for word (db::sqlite::REAPABLE). A run
-- that waits for its turn at a cap cannot be over (its tasks are held
-- pending until it is let in), so the slice need not look at one.
CREATE TABLE IF NOT EXISTS run_wakes (
    run_id TEXT PRIMARY KEY NOT NULL
) WITHOUT ROWID;

-- A task ended: from a status not over to one that is. Written by the
-- finished statuses alone, so a status added later that is not over cannot
-- slip past it.
CREATE TRIGGER IF NOT EXISTS run_wakes_task_ended
AFTER UPDATE OF status ON task_runs
WHEN NEW.status IN ('succeeded', 'failed', 'skipped', 'cancelled')
 AND OLD.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
BEGIN
    INSERT OR IGNORE INTO run_wakes (run_id) VALUES (NEW.run_id);
END;

-- A task made already over. No path writes one today (create_run writes
-- `pending`, the fan-out `ready`); this costs nothing and covers one later.
CREATE TRIGGER IF NOT EXISTS run_wakes_task_made_over
AFTER INSERT ON task_runs
WHEN NEW.status IN ('succeeded', 'failed', 'skipped', 'cancelled')
BEGIN
    INSERT OR IGNORE INTO run_wakes (run_id) VALUES (NEW.run_id);
END;

-- A task not yet over taken away from a running run: a fan-out's instances
-- re-expanded (db::sqlite reconcile_fanouts, a rerun's reset). GC deletes
-- the tasks of finished runs, which the run's status leaves out; the task's
-- own status is tested first, so a delete of finished tasks never probes.
CREATE TRIGGER IF NOT EXISTS run_wakes_task_gone
AFTER DELETE ON task_runs
WHEN OLD.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
 AND EXISTS (SELECT 1 FROM workflow_runs WHERE id = OLD.run_id AND status = 'running')
BEGIN
    INSERT OR IGNORE INTO run_wakes (run_id) VALUES (OLD.run_id);
END;

-- A run made running and not waiting for its turn: one with no task is over
-- at once (a `when:` of its parameters left every task out).
CREATE TRIGGER IF NOT EXISTS run_wakes_run_made
AFTER INSERT ON workflow_runs
WHEN NEW.status = 'running' AND NEW.stopping_as IS NULL
 AND (NEW.queued_at IS NULL OR NEW.admitted_at IS NOT NULL)
BEGIN
    INSERT OR IGNORE INTO run_wakes (run_id) VALUES (NEW.id);
END;

-- A run that has become one the reap may end: let in at its cap, running
-- again (a retry, a clear, a rerun), or a stop taken back.
CREATE TRIGGER IF NOT EXISTS run_wakes_run_changed
AFTER UPDATE OF status, stopping_as, queued_at, admitted_at ON workflow_runs
WHEN NEW.status = 'running' AND NEW.stopping_as IS NULL
 AND (NEW.queued_at IS NULL OR NEW.admitted_at IS NOT NULL)
 AND (OLD.status IS NOT NEW.status
      OR OLD.stopping_as IS NOT NEW.stopping_as
      OR OLD.queued_at IS NOT NEW.queued_at
      OR OLD.admitted_at IS NOT NEW.admitted_at)
BEGIN
    INSERT OR IGNORE INTO run_wakes (run_id) VALUES (NEW.id);
END;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_reapable
    ON workflow_runs (status, id)
    WHERE status = 'running' AND stopping_as IS NULL
      AND (queued_at IS NULL OR admitted_at IS NOT NULL);

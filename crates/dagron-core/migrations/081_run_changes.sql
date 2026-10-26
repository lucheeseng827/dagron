-- What dagron-api's live stream reads on SQLite. It
-- read every run not finished or finished in the last 30 s, with its tasks'
-- statuses, every DAGRON_SQLITE_POLL_MS (500 ms), and sent each run whose
-- status, or a task's status or attempt, had changed: a read of every run
-- kept, on the one connection dagron-api has, twice a second. Here the file
-- writes each run such a change touched to `run_changes`, whoever made the
-- change, in the transaction of the change; the stream keeps a cursor and
-- reads past it (db::sqlite::run_changes_after).
--
-- One row a run: a change deletes the run's row and writes it again, with a
-- `seq` past every one written before (AUTOINCREMENT never takes one back).
-- SQLite has one writer at a time, so a change is visible to a reader in
-- the order of its `seq`: a reader that has read up to a `seq` has missed
-- none below it. Nothing reads the table but to read it: it is not
-- 079's run_wakes, which the engine empties, and any number of readers
-- (two dagron-api processes on the file) keep their own cursors.
--
-- A DELETE and an INSERT, not INSERT OR REPLACE: a trigger's statements take
-- the conflict clause of the statement that fired them, and under an
-- outer OR IGNORE a REPLACE would be ignored, and the change lost.
--
-- Its bound: each 1,024th change takes away the rows 65,536 or more changes
-- behind it (db::sqlite::RUN_CHANGES_KEPT), so the table holds at most
-- 66,560 rows, and no writer or reader need prune it. A row that more than
-- 65,536 changes have passed may be gone, so a reader whose cursor is that
-- far behind the newest `seq` reads every live run again, as at its start.
--
-- What is written, as the stream compared it before: a run made, a run's
-- status changed, a task made, a task's status or attempt changed, and a
-- task not over taken away (a fan-out's instances re-expanded). The tasks
-- GC takes away are over, and are not written.
CREATE TABLE IF NOT EXISTS run_changes (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE
);

CREATE TRIGGER IF NOT EXISTS run_changes_run_made
AFTER INSERT ON workflow_runs
BEGIN
    DELETE FROM run_changes WHERE run_id = NEW.id;
    INSERT INTO run_changes (run_id) VALUES (NEW.id);
END;

CREATE TRIGGER IF NOT EXISTS run_changes_run_moved
AFTER UPDATE OF status ON workflow_runs
WHEN OLD.status IS NOT NEW.status
BEGIN
    DELETE FROM run_changes WHERE run_id = NEW.id;
    INSERT INTO run_changes (run_id) VALUES (NEW.id);
END;

CREATE TRIGGER IF NOT EXISTS run_changes_task_made
AFTER INSERT ON task_runs
BEGIN
    DELETE FROM run_changes WHERE run_id = NEW.run_id;
    INSERT INTO run_changes (run_id) VALUES (NEW.run_id);
END;

CREATE TRIGGER IF NOT EXISTS run_changes_task_moved
AFTER UPDATE OF status, attempt ON task_runs
WHEN OLD.status IS NOT NEW.status OR OLD.attempt IS NOT NEW.attempt
BEGIN
    DELETE FROM run_changes WHERE run_id = NEW.run_id;
    INSERT INTO run_changes (run_id) VALUES (NEW.run_id);
END;

CREATE TRIGGER IF NOT EXISTS run_changes_task_gone
AFTER DELETE ON task_runs
WHEN OLD.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
BEGIN
    DELETE FROM run_changes WHERE run_id = OLD.run_id;
    INSERT INTO run_changes (run_id) VALUES (OLD.run_id);
END;

CREATE TRIGGER IF NOT EXISTS run_changes_bounded
AFTER INSERT ON run_changes
WHEN NEW.seq % 1024 = 0
BEGIN
    DELETE FROM run_changes WHERE seq <= NEW.seq - 65536;
END;

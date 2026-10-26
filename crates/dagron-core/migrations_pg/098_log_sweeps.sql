-- The log store's GC. Everything the log store
-- (DAGRON_LOG_STORE) keeps of a task is under one name of its own, and goes
-- once no reader can name it: a whole task once its row is gone, a writing
-- of its log once a later one took its place. See LOG_PIPELINE §7.4.
--
-- `task_runs.log_writings`: how many writings of the task's log were begun
-- in the store, over all its attempts; NULL where none was (no store, a
-- remote runner's task, a task that never ran). Never set back. Nullable
-- with no default: no table rewrite. Rows whose segments were closed before
-- this are marked here.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS log_writings BIGINT;
UPDATE task_runs SET log_writings = 1
 WHERE log_writings IS NULL AND id IN (SELECT DISTINCT task_id FROM log_segments);

-- A task whose store may hold what no reader names, until an engine with the
-- store has removed it and looked again: written in the statement or the
-- transaction that made it so (the trigger below; `db::reset_task_log`).
-- `reason`: 'gone' (the row was deleted), 'superseded' (a writing was begun
-- again), or 'orphan' (by hand). Times are the database's clock, as the
-- outbox's trigger writes them (090). `stamp` moves on at every write, claim
-- and finish, and a finish applies only at the stamp it claimed. No foreign
-- key: a record outlives its task's row.
CREATE TABLE IF NOT EXISTS log_sweeps (
    task_id     TEXT PRIMARY KEY,
    run_id      TEXT,
    reason      TEXT NOT NULL,
    marked_at   TEXT NOT NULL,
    due_at      TEXT NOT NULL,
    stamp       BIGINT NOT NULL DEFAULT 1,
    tries       BIGINT NOT NULL DEFAULT 0,
    last_error  TEXT
);
CREATE INDEX IF NOT EXISTS idx_log_sweeps_due ON log_sweeps (due_at);
CREATE INDEX IF NOT EXISTS idx_log_sweeps_superseded_due ON log_sweeps (due_at)
    WHERE reason = 'superseded';

-- A trigger, so that every path a task row is deleted on is covered: the
-- retention GC, an archive's purge (the engine's and dagron-api's), a rerun's
-- and a fan-out's drops, and a DELETE by hand. Row by row, and only for a row
-- whose log the store holds: a row with none costs nothing. Due a minute on,
-- for an engine still writing the task's last lines; a record due sooner
-- stays due then.
CREATE OR REPLACE FUNCTION log_sweep_gone() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    at  TEXT := to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"');
    due TEXT := to_char((clock_timestamp() + interval '60 seconds') AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"');
BEGIN
    INSERT INTO log_sweeps (task_id, run_id, reason, marked_at, due_at)
    VALUES (OLD.id, OLD.run_id, 'gone', at, due)
    ON CONFLICT (task_id) DO UPDATE SET reason = 'gone', run_id = EXCLUDED.run_id,
        marked_at = EXCLUDED.marked_at, due_at = LEAST(log_sweeps.due_at, EXCLUDED.due_at),
        stamp = log_sweeps.stamp + 1, tries = 0, last_error = NULL;
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS task_runs_log_sweep ON task_runs;
CREATE TRIGGER task_runs_log_sweep AFTER DELETE ON task_runs
    FOR EACH ROW WHEN (OLD.log_writings IS NOT NULL)
    EXECUTE FUNCTION log_sweep_gone();

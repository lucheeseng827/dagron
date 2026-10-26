-- A task that ends tells its run's listeners, whoever ended it, as SQLite's
-- 079 writes the run to run_wakes.
--
-- The reconcile loop reaps the runs a notification on `task_events` named
-- (db::postgres::Waker::take_woken), and the reap's slice, a slice of the
-- running runs each REAP_INTERVAL_MS (5 s), is the net under one that was
-- lost. Each function that ends a task sent its own `pg_notify`, and some
-- sent none: a fan-out's barrier finished, the skips of an advance after a
-- failure, the time, HTTP and dataset sensors resolved, an external job's
-- end, a sub-workflow's trigger resolved. Such a run was ended by the slice,
-- up to REAP_INTERVAL_MS for each 500 running runs late, and counted in
-- scheduler_runs_reaped_by_sweep_total. A trigger covers every path that
-- ends a task, those to come and dagron-api's own SQL among them.
--
-- The payload is the run, on the channel the engine and dagron-api's SSE
-- bridge LISTEN on, as every other notification of a run. A notification
-- is sent at its transaction's commit, and Postgres folds those of one
-- transaction with the same channel and payload into one: a cancel of a
-- run's ten thousand tasks is one notification, and the functions that
-- notify in the transaction that ends the task add none. Those that notify
-- after it, in a transaction of their own, are heard twice, which the
-- engine takes in one tick.
--
-- What it fires on, by the finished statuses alone, as 079: a status
-- changed to a finished one from one that is not; a task made finished;
-- and a task not finished taken away (a fan-out's instances re-expanded).
-- The WHEN is tested before the function is called, so an update of a
-- task's lease, its log or its output costs nothing, and GC, which takes
-- away finished tasks, calls nothing.
CREATE OR REPLACE FUNCTION task_runs_notify_run() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('task_events', OLD.run_id);
    ELSE
        PERFORM pg_notify('task_events', NEW.run_id);
    END IF;
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS task_runs_ended_notify ON task_runs;
CREATE TRIGGER task_runs_ended_notify AFTER UPDATE OF status ON task_runs
    FOR EACH ROW
    WHEN (NEW.status IN ('succeeded', 'failed', 'skipped', 'cancelled')
          AND OLD.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled'))
    EXECUTE FUNCTION task_runs_notify_run();

DROP TRIGGER IF EXISTS task_runs_made_over_notify ON task_runs;
CREATE TRIGGER task_runs_made_over_notify AFTER INSERT ON task_runs
    FOR EACH ROW
    WHEN (NEW.status IN ('succeeded', 'failed', 'skipped', 'cancelled'))
    EXECUTE FUNCTION task_runs_notify_run();

DROP TRIGGER IF EXISTS task_runs_gone_notify ON task_runs;
CREATE TRIGGER task_runs_gone_notify AFTER DELETE ON task_runs
    FOR EACH ROW
    WHEN (OLD.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled'))
    EXECUTE FUNCTION task_runs_notify_run();

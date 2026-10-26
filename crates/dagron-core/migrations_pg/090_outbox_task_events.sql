-- 10.22: a task's start and end in the outbox, for a sink that asks for them
-- (dagron-ci's reporter, the `ci` sink), and only while one does.
--
-- `outbox_sinks.wants_tasks_until`: until when the sink is handed
-- `task.started` and `task.finished`: a day past its last claim, renewed as
-- it claims (`db::claim_outbox_batch`). NULL for a sink that never asked. A
-- sink that stops claiming for good stops costing every task a row a day
-- later.
ALTER TABLE outbox_sinks ADD COLUMN IF NOT EXISTS wants_tasks_until TEXT;

-- A trigger, so that every path a task's status changes on is covered: the
-- claims, the results, a cancel through either API, the sweeps. One row of
-- `outbox_sinks` is read where no sink wants them, and nothing is written.
-- `clock_timestamp()`, not `now()`: the events of one transaction are
-- written in the order they happened.
CREATE OR REPLACE FUNCTION outbox_task_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    at TEXT := to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"');
BEGIN
    IF EXISTS (SELECT 1 FROM outbox_sinks WHERE wants_tasks_until > at) THEN
        INSERT INTO event_outbox (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
        VALUES (
            gen_random_uuid()::text,
            NEW.run_id,
            CASE NEW.status WHEN 'running' THEN 'task.started'
                            WHEN 'awaiting_approval' THEN 'task.waiting'
                            ELSE 'task.finished' END,
            json_build_object('run_id', NEW.run_id, 'task_id', NEW.id, 'name', NEW.name,
                              'status', NEW.status, 'attempt', NEW.attempt)::text,
            'pending', 0, at, at
        );
    END IF;
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS task_runs_outbox ON task_runs;
CREATE TRIGGER task_runs_outbox AFTER UPDATE OF status ON task_runs
    FOR EACH ROW
    WHEN (NEW.status IS DISTINCT FROM OLD.status
          AND NEW.status IN ('running', 'awaiting_approval', 'succeeded', 'failed', 'cancelled', 'skipped'))
    EXECUTE FUNCTION outbox_task_event();

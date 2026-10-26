-- 10.22: a task's start and end in the outbox, for a sink that asks for them,
-- and only while one does. See migrations_pg/090_outbox_task_events.sql.
ALTER TABLE outbox_sinks ADD COLUMN wants_tasks_until TEXT;

CREATE TRIGGER IF NOT EXISTS task_runs_outbox AFTER UPDATE OF status ON task_runs
    WHEN NEW.status IS NOT OLD.status
     AND NEW.status IN ('running', 'awaiting_approval', 'succeeded', 'failed', 'cancelled', 'skipped')
     AND EXISTS (SELECT 1 FROM outbox_sinks
                 WHERE wants_tasks_until > strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
BEGIN
    INSERT INTO event_outbox (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
    VALUES (
        lower(hex(randomblob(16))),
        NEW.run_id,
        CASE NEW.status WHEN 'running' THEN 'task.started'
                        WHEN 'awaiting_approval' THEN 'task.waiting'
                        ELSE 'task.finished' END,
        json_object('run_id', NEW.run_id, 'task_id', NEW.id, 'name', NEW.name,
                    'status', NEW.status, 'attempt', NEW.attempt),
        'pending', 0,
        strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'),
        strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
    );
END;

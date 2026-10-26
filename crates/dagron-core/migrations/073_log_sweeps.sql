-- The log store's GC. See
-- migrations_pg/098_log_sweeps.sql, which says what each part is for.
ALTER TABLE task_runs ADD COLUMN log_writings INTEGER;
UPDATE task_runs SET log_writings = 1
 WHERE log_writings IS NULL AND id IN (SELECT DISTINCT task_id FROM log_segments);

CREATE TABLE IF NOT EXISTS log_sweeps (
    task_id     TEXT PRIMARY KEY,
    run_id      TEXT,
    reason      TEXT NOT NULL,
    marked_at   TEXT NOT NULL,
    due_at      TEXT NOT NULL,
    stamp       INTEGER NOT NULL DEFAULT 1,
    tries       INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT
);
CREATE INDEX IF NOT EXISTS idx_log_sweeps_due ON log_sweeps (due_at);
CREATE INDEX IF NOT EXISTS idx_log_sweeps_superseded_due ON log_sweeps (due_at)
    WHERE reason = 'superseded';

CREATE TRIGGER IF NOT EXISTS task_runs_log_sweep AFTER DELETE ON task_runs
    WHEN OLD.log_writings IS NOT NULL
BEGIN
    INSERT INTO log_sweeps (task_id, run_id, reason, marked_at, due_at)
    VALUES (OLD.id, OLD.run_id, 'gone',
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'),
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', '+60 seconds'))
    ON CONFLICT (task_id) DO UPDATE SET reason = 'gone', run_id = excluded.run_id,
        marked_at = excluded.marked_at, due_at = min(due_at, excluded.due_at),
        stamp = stamp + 1, tries = 0, last_error = NULL;
END;

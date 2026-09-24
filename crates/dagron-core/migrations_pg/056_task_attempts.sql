-- Mirrors migrations/044_task_attempts.sql (SQLite). See there for the full
-- reasoning: `task_runs.output` is singular and overwritten by every attempt,
-- so the log has only ever shown the last one; this table holds the superseded
-- attempts as a bounded tail, and the live attempt stays where it was.
CREATE TABLE IF NOT EXISTS task_attempts (
    -- ON DELETE CASCADE: the retention sweep deletes task_runs rows directly,
    -- and a plain reference would make every purge of a run that looped a
    -- foreign-key violation. See the SQLite mirror.
    task_id     TEXT NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    attempt     BIGINT NOT NULL,
    reason      TEXT NOT NULL,
    output      TEXT,
    truncated   BOOLEAN NOT NULL DEFAULT FALSE,
    finished_at TEXT NOT NULL,
    PRIMARY KEY (task_id, attempt)
);

-- Mirrors migrations_pg/0019_schedules.sql. The comments there say what the
-- columns and the table are for.
ALTER TABLE ci_repos ADD COLUMN schedule_due_at TEXT;
ALTER TABLE ci_repos ADD COLUMN schedule_claim  TEXT;
ALTER TABLE ci_repos ADD COLUMN schedule_sha    TEXT;
ALTER TABLE ci_repos ADD COLUMN schedule_error  TEXT;
CREATE INDEX ci_repos_schedule_due ON ci_repos (schedule_due_at);

CREATE TABLE ci_schedules (
    repo_id  TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    workflow TEXT NOT NULL,
    cron     TEXT NOT NULL,
    next_at  INTEGER,
    last_at  INTEGER,
    PRIMARY KEY (repo_id, workflow, cron)
);

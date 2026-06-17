-- dagron v7 (UI) — workflow schedules (Postgres).
--
-- Mirrors migrations/006_schedules.sql. A schedule pairs a first-class workflow
-- with a cron expression. dagron-api manages the rows (the UI "schedule
-- drawer"); the engine's leadership-gated schedule loop fires due rows by
-- submitting the workflow's spec via create_run. `next_fire_at` lives here in
-- shared state, so only the current leader advances it — no double-fire across
-- N schedulers. `enabled` is BIGINT (0/1) to match the i64 binding code (the
-- SQLite path stores it in a dynamically-typed INTEGER column).

CREATE TABLE IF NOT EXISTS schedules (
    id            TEXT PRIMARY KEY NOT NULL,
    workflow_id   TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    cron_expr     TEXT NOT NULL,
    enabled       BIGINT NOT NULL DEFAULT 1,
    next_fire_at  TEXT,
    last_fired_at TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

-- The engine's fire sweep selects enabled schedules due to run.
CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON schedules (next_fire_at)
    WHERE enabled = 1;
CREATE INDEX IF NOT EXISTS idx_schedules_workflow ON schedules(workflow_id);

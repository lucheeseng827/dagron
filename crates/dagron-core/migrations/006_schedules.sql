-- dagron v7 (UI) — workflow schedules (SQLite).
--
-- Mirrors migrations_pg/006_schedules.sql. Pairs a first-class workflow with a
-- cron expression; dagron-api manages the rows (UI schedule drawer), the engine's
-- leadership-gated loop fires due rows via create_run. ON DELETE CASCADE relies
-- on `PRAGMA foreign_keys = ON` (set at pool init).

CREATE TABLE IF NOT EXISTS schedules (
    id            TEXT PRIMARY KEY NOT NULL,
    workflow_id   TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    cron_expr     TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    next_fire_at  TEXT,
    last_fired_at TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_due
    ON schedules (next_fire_at)
    WHERE enabled = 1;
CREATE INDEX IF NOT EXISTS idx_schedules_workflow ON schedules(workflow_id);

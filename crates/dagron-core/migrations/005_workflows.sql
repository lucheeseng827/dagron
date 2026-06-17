-- dagron v7 (UI) — first-class workflows (SQLite).
--
-- Mirrors migrations_pg/005_workflows.sql. Named, reusable DAG definitions
-- managed through the UI / dagron-api (distinct from the engine's per-run
-- `workflow_definitions`). Running a workflow submits its `spec` via create_run.

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL UNIQUE,
    spec        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflows_name ON workflows(name);

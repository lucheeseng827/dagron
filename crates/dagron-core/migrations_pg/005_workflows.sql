-- dagron v7 (UI) — first-class workflows (Postgres).
--
-- Mirrors migrations/005_workflows.sql. Distinct from `workflow_definitions`
-- (which the engine writes per run, ephemeral): `workflows` are named, reusable
-- DAG definitions managed through the UI / dagron-api. "Running" a workflow
-- submits its `spec` via the normal create_run path, producing an ordinary run.
-- The engine does not read this table; only dagron-api does.

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL UNIQUE,
    spec        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflows_name ON workflows(name);

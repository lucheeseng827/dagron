-- Datasets: data-aware scheduling substrate (Airflow Datasets / Dagster asset
-- sensors) — registry + append-only lineage ledger + trigger subscriptions.
-- Firing is a CAS advance on dataset_triggers.cursor (HA-safe, no leadership).
-- Mirrors migrations/032_datasets.sql; `id` is BIGSERIAL for the same
-- monotonic-cursor contract.
CREATE TABLE IF NOT EXISTS datasets (
    uri          TEXT PRIMARY KEY,
    updated_at   TEXT NOT NULL,
    last_run_id  TEXT,
    last_task    TEXT,
    updates      BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dataset_events (
    id        BIGSERIAL PRIMARY KEY,
    uri       TEXT NOT NULL,
    workflow  TEXT,
    run_id    TEXT,
    task_id   TEXT,
    task_name TEXT,
    source    TEXT NOT NULL DEFAULT 'task',
    at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dataset_events_uri_id ON dataset_events (uri, id);

CREATE TABLE IF NOT EXISTS dataset_triggers (
    workflow_name     TEXT NOT NULL,
    uri               TEXT NOT NULL,
    cursor            BIGINT NOT NULL DEFAULT 0,
    mode              TEXT NOT NULL DEFAULT 'any',
    last_fired_at     TEXT,
    last_fired_run_id TEXT,
    PRIMARY KEY (workflow_name, uri)
);

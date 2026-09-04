-- Datasets: data-aware scheduling substrate (Airflow Datasets / Dagster asset
-- sensors). Three tables:
--
-- * `datasets` — the registry: one row per dataset URI ever produced, with its
--   latest update (who, when, how many times). Upserted when a task with a
--   `produces:` entry succeeds.
-- * `dataset_events` — the append-only lineage ledger: one row per update, with
--   the producing workflow/run/task. `id` is monotonically increasing — dataset
--   sensors and triggers key off "an event with id > my cursor exists", so
--   consumers never re-read history and coalescing is natural.
-- * `dataset_triggers` — subscriptions: (workflow, dataset) edges derived from
--   registered workflows' `on_datasets:`, each with a `cursor` (the last
--   dataset_events.id consumed). Firing is a CAS advance on the cursor —
--   HA-safe with no leadership: only the scheduler that wins the UPDATE
--   creates the run. The open build subscribes one dataset per workflow;
--   multi-dataset composition is not in this build.
CREATE TABLE IF NOT EXISTS datasets (
    uri          TEXT PRIMARY KEY,
    updated_at   TEXT NOT NULL,
    last_run_id  TEXT,
    last_task    TEXT,
    updates      BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dataset_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    uri       TEXT NOT NULL,
    workflow  TEXT,
    run_id    TEXT,
    task_id   TEXT,
    task_name TEXT,
    -- 'task' (a produces: success) or 'external' (feature-gated: posted via the API).
    source    TEXT NOT NULL DEFAULT 'task',
    at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dataset_events_uri_id ON dataset_events (uri, id);

CREATE TABLE IF NOT EXISTS dataset_triggers (
    workflow_name     TEXT NOT NULL,
    uri               TEXT NOT NULL,
    cursor            BIGINT NOT NULL DEFAULT 0,
    -- 'any' (fire on any subscribed dataset's update) or 'all' (fire once every
    -- subscribed dataset updated since the last fire — composition under the `enterprise` feature).
    mode              TEXT NOT NULL DEFAULT 'any',
    last_fired_at     TEXT,
    last_fired_run_id TEXT,
    PRIMARY KEY (workflow_name, uri)
);

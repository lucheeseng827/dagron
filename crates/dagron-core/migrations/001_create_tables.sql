-- Workflow scheduler v0 schema.
-- task_dependencies tracks edges so we can decrement remaining_deps atomically
-- without re-walking the DAG on every tick.

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    spec        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id              TEXT PRIMARY KEY NOT NULL,
    definition_id   TEXT NOT NULL REFERENCES workflow_definitions(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','running','succeeded','failed','cancelled')),
    input           TEXT,
    output          TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    id               TEXT PRIMARY KEY NOT NULL,
    run_id           TEXT NOT NULL REFERENCES workflow_runs(id),
    name             TEXT NOT NULL,
    -- The column that drives the whole state machine.
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK(status IN ('pending','ready','running','succeeded','failed','skipped','cancelled')),
    attempt          INTEGER NOT NULL DEFAULT 0,
    -- Dependency counter: flip to 'ready' when this hits 0 (see advance_ready_tasks).
    remaining_deps   INTEGER NOT NULL DEFAULT 0,
    -- Full TaskSpec JSON stored here so the row is self-contained after dispatch.
    input            TEXT,
    output           TEXT,
    -- Lease columns: the correctness anchor for crash recovery.
    claimed_by       TEXT,
    lease_expires_at TEXT,
    -- Optimistic concurrency: CAS on (id, version) prevents double-claim.
    version          INTEGER NOT NULL DEFAULT 0,
    scheduled_at     TEXT,
    finished_at      TEXT
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    dependent_id  TEXT NOT NULL REFERENCES task_runs(id),
    dependency_id TEXT NOT NULL REFERENCES task_runs(id),
    PRIMARY KEY (dependent_id, dependency_id)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_run_id  ON task_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_task_runs_status  ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_task_deps_dep_id  ON task_dependencies(dependency_id);

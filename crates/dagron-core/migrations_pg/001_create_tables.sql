-- Workflow scheduler v2 schema (Postgres).
--
-- Mirrors migrations/001_create_tables.sql (SQLite) one-for-one. Columns are kept
-- as plain TEXT (timestamps stored as RFC-3339 strings, status as text) so the
-- Rust binding code is identical across both backends — the only behavioural
-- difference lives in db::postgres::claim_ready (FOR UPDATE SKIP LOCKED). Integer
-- counters are BIGINT to match the i64 fields on models::TaskRun.

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    spec        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (now()::text)
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id              TEXT PRIMARY KEY NOT NULL,
    definition_id   TEXT NOT NULL REFERENCES workflow_definitions(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','running','succeeded','failed','cancelled')),
    input           TEXT,
    output          TEXT,
    created_at      TEXT NOT NULL DEFAULT (now()::text),
    finished_at     TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    id               TEXT PRIMARY KEY NOT NULL,
    run_id           TEXT NOT NULL REFERENCES workflow_runs(id),
    name             TEXT NOT NULL,
    -- The column that drives the whole state machine.
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK(status IN ('pending','ready','running','succeeded','failed','skipped','cancelled')),
    attempt          BIGINT NOT NULL DEFAULT 0,
    -- Dependency counter: flip to 'ready' when this hits 0 (see advance_ready_tasks).
    remaining_deps   BIGINT NOT NULL DEFAULT 0,
    -- Full TaskSpec JSON stored here so the row is self-contained after dispatch.
    input            TEXT,
    output           TEXT,
    -- Lease columns: the correctness anchor for crash recovery.
    claimed_by       TEXT,
    lease_expires_at TEXT,
    -- Optimistic concurrency guard, retained for parity with the SQLite path;
    -- the Postgres claim path relies on FOR UPDATE SKIP LOCKED rather than CAS.
    version          BIGINT NOT NULL DEFAULT 0,
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

-- Hot-path partial indexes for the reconcile loop's two contended scans:
--   claim_ready:            WHERE status='ready'   ORDER BY scheduled_at
--   recover_expired_leases: WHERE status='running' AND lease_expires_at < now
-- Partial indexes keep them tiny (only the rows in flight) and let the planner
-- satisfy the order/filter without scanning the whole table under load.
CREATE INDEX IF NOT EXISTS idx_task_runs_ready_scheduled_at
    ON task_runs (scheduled_at, id)
    WHERE status = 'ready';
CREATE INDEX IF NOT EXISTS idx_task_runs_running_lease_expires_at
    ON task_runs (lease_expires_at)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;

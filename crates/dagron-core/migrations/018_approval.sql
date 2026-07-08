-- Human approval gates (fast-win #19).
--
-- A `type: approval` task carries `is_approval = 1`; when its dependencies are
-- satisfied the scheduler parks it in the new `awaiting_approval` status instead
-- of dispatching a command, until an operator approves/rejects it via the API or
-- its timeout auto-resolves it. `approval_timeout_secs` + `approval_on_timeout`
-- ('approve'|'reject', default reject) drive the timeout sweep; the "awaiting
-- since" instant reuses `task_runs.scheduled_at`, stamped on entry.
--
-- SQLite can't ALTER a CHECK constraint, and the status column has one, so the
-- table is rebuilt to widen it AND add the approval columns in one step.
-- `task_dependencies` has a foreign key into `task_runs`, so its edges are staged
-- and it is dropped before `task_runs` is recreated, then restored — the standard
-- SQLite table-rebuild dance. All-or-nothing under the migration transaction.

CREATE TABLE _task_deps_bak AS SELECT * FROM task_dependencies;
DROP TABLE task_dependencies;

CREATE TABLE task_runs_new (
    id               TEXT PRIMARY KEY NOT NULL,
    run_id           TEXT NOT NULL REFERENCES workflow_runs(id),
    name             TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK(status IN ('pending','ready','running','awaiting_approval',
                                      'succeeded','failed','skipped','cancelled')),
    attempt          INTEGER NOT NULL DEFAULT 0,
    remaining_deps   INTEGER NOT NULL DEFAULT 0,
    input            TEXT,
    output           TEXT,
    claimed_by       TEXT,
    lease_expires_at TEXT,
    version          INTEGER NOT NULL DEFAULT 0,
    scheduled_at     TEXT,
    finished_at      TEXT,
    trigger_rule     TEXT NOT NULL DEFAULT 'all_success',
    allow_failure    INTEGER NOT NULL DEFAULT 0,
    is_approval      INTEGER NOT NULL DEFAULT 0,
    approval_timeout_secs INTEGER,
    approval_on_timeout   TEXT
);

INSERT INTO task_runs_new
    (id, run_id, name, status, attempt, remaining_deps, input, output,
     claimed_by, lease_expires_at, version, scheduled_at, finished_at,
     trigger_rule, allow_failure)
SELECT
    id, run_id, name, status, attempt, remaining_deps, input, output,
    claimed_by, lease_expires_at, version, scheduled_at, finished_at,
    trigger_rule, allow_failure
FROM task_runs;

DROP TABLE task_runs;
ALTER TABLE task_runs_new RENAME TO task_runs;

CREATE INDEX idx_task_runs_run_id ON task_runs(run_id);
CREATE INDEX idx_task_runs_status ON task_runs(status);
CREATE UNIQUE INDEX idx_task_runs_run_id_name ON task_runs(run_id, name);

CREATE TABLE task_dependencies (
    dependent_id  TEXT NOT NULL REFERENCES task_runs(id),
    dependency_id TEXT NOT NULL REFERENCES task_runs(id),
    PRIMARY KEY (dependent_id, dependency_id)
);
INSERT INTO task_dependencies (dependent_id, dependency_id)
    SELECT dependent_id, dependency_id FROM _task_deps_bak;
DROP TABLE _task_deps_bak;
CREATE INDEX idx_task_deps_dep_id ON task_dependencies(dependency_id);

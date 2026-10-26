-- Session leases, and what a remote runner says of a task.
-- See migrations_pg/091_runner_sessions.sql. Runners dial dagron-api, which
-- is Postgres's alone, so a SQLite engine never has a session: these are
-- here so that its sweeps and its result intake are the same statements.
-- `labels` is a comma-separated list where Postgres has an array. The
-- runners and their join tokens are dagron-api's, and are not here.
CREATE TABLE IF NOT EXISTS runner_sessions (
    id               TEXT PRIMARY KEY,
    runner_id        TEXT NOT NULL,
    holder           TEXT NOT NULL,
    labels           TEXT NOT NULL,
    capacity         INTEGER NOT NULL,
    version          TEXT,
    protocol         INTEGER NOT NULL,
    secrets          TEXT NOT NULL,
    os               TEXT,
    arch             TEXT,
    lease_expires_at TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    draining_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_runner_sessions_lease ON runner_sessions (lease_expires_at);

ALTER TABLE task_runs ADD COLUMN session_id TEXT;
CREATE INDEX IF NOT EXISTS idx_task_runs_session
    ON task_runs (session_id)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS runner_results (
    task_id    TEXT PRIMARY KEY,
    fence      INTEGER NOT NULL,
    claimed_by TEXT NOT NULL,
    status     TEXT NOT NULL,
    exit_code  INTEGER,
    output     TEXT,
    created_at TEXT NOT NULL,
    taken_by   TEXT,
    taken_at   TEXT
);

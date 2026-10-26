-- The remote runner, and session leases.
--
-- A runner is a host that dials dagron-api, takes the tasks of the classes it
-- serves, runs them, and says how they came out. It holds no database
-- credentials. It enrols once with a join token an admin minted
-- (`runner_join_tokens`), and is a principal of its own from then on,
-- `runner:<id>` (`runners`): its credential is stored as an API token is,
-- its prefix to find it by and its SHA-256 to compare.
--
-- A session is one runner process's hold on the tasks it has taken
-- (`runner_sessions`). Its heartbeat renews the session's lease, and with it
-- every task the session holds: a task row is not rewritten on heartbeat.
-- A task the session holds points at it (`task_runs.session_id`), and keeps
-- the non-NULL lease it was claimed with, so that what reads a NULL lease as
-- "parked" keeps its meaning. The engine's expired-lease sweeps pass such a
-- row by; the session sweep takes it back to `ready` once its session's
-- lease has lapsed.
--
-- What a runner says of a task is kept in `runner_results` until an engine
-- takes it and applies it as it applies a result of its own workers: retries,
-- `repeat:`, `defer:`, the cache, a dead letter. One row a task: a task has
-- one claim at a time.
--
-- New tables, and a nullable column, so nothing is rewritten. The column's
-- index is built in 092, concurrently.
CREATE TABLE IF NOT EXISTS runner_join_tokens (
    id          TEXT PRIMARY KEY,
    prefix      TEXT NOT NULL,
    token_hash  TEXT NOT NULL,
    labels      TEXT[] NOT NULL,
    max_uses    INTEGER NOT NULL,
    uses        INTEGER NOT NULL DEFAULT 0,
    created_by  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    revoked_at  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runner_join_tokens_prefix
    ON runner_join_tokens (prefix);

CREATE TABLE IF NOT EXISTS runners (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    prefix             TEXT NOT NULL,
    token_hash         TEXT NOT NULL,
    labels             TEXT[] NOT NULL,
    os                 TEXT,
    arch               TEXT,
    capacity           INTEGER NOT NULL,
    version            TEXT,
    join_token_id      TEXT,
    created_at         TEXT NOT NULL,
    last_seen_at       TEXT,
    revoked_at         TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runners_prefix ON runners (prefix);

CREATE TABLE IF NOT EXISTS runner_sessions (
    id               TEXT PRIMARY KEY,
    runner_id        TEXT NOT NULL,
    -- What the session's tasks are claimed by (`task_runs.claimed_by`):
    -- `runner:<runner id>/<session id>`, one per session, so a word of an
    -- earlier session of the same runner is never taken for this one's.
    holder           TEXT NOT NULL,
    labels           TEXT[] NOT NULL,
    capacity         INTEGER NOT NULL,
    version          TEXT,
    protocol         INTEGER NOT NULL,
    -- `server`: dagron-api resolves a task's secrets and sends them. `home`:
    -- the runner resolves them from its own store, and none is sent.
    secrets          TEXT NOT NULL,
    os               TEXT,
    arch             TEXT,
    lease_expires_at TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    draining_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_runner_sessions_runner ON runner_sessions (runner_id);
CREATE INDEX IF NOT EXISTS idx_runner_sessions_lease ON runner_sessions (lease_expires_at);

ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS session_id TEXT;

CREATE TABLE IF NOT EXISTS runner_results (
    task_id    TEXT PRIMARY KEY,
    fence      BIGINT NOT NULL,
    claimed_by TEXT NOT NULL,
    -- succeeded | failed | timed_out | backpressure | cancelled
    status     TEXT NOT NULL,
    exit_code  INTEGER,
    output     TEXT,
    created_at TEXT NOT NULL,
    -- Which engine took it, and when: one that took it and did not apply it
    -- (it died) is passed over once a minute has gone by.
    taken_by   TEXT,
    taken_at   TEXT
);

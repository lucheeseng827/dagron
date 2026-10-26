-- What dagron-api keeps beside the engine's tables, for a one-node install on
-- SQLite. The Postgres set has had them since
-- migrations_pg 008 (users), 009 (the GitOps repositories), 010 (a workflow's
-- description), 060 (who manages a workflow from Git) and 062 (who triggered
-- a run); dagron-api reads and writes them, and the engine never does.

CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    pw_hash     TEXT NOT NULL,
    groups      TEXT NOT NULL DEFAULT '[]',
    created_at  TEXT NOT NULL
);

-- The GitOps repositories, with the columns dagron-api's Postgres bootstrap
-- adds (routes/gitrepos.rs). A one-node build connects none (its Git routes
-- are the Postgres build's), but a workflow's edit asks whether one manages
-- it.
CREATE TABLE IF NOT EXISTS git_repos (
    id                TEXT PRIMARY KEY NOT NULL,
    name              TEXT NOT NULL,
    url               TEXT NOT NULL UNIQUE,
    branch            TEXT NOT NULL DEFAULT 'main',
    rev               TEXT,
    state             TEXT NOT NULL DEFAULT 'OutOfSync',
    auto_sync         BIGINT NOT NULL DEFAULT 0,
    workflow_count    BIGINT NOT NULL DEFAULT 0,
    drift             BIGINT NOT NULL DEFAULT 0,
    last_message      TEXT,
    last_synced_at    TEXT,
    created_at        TEXT NOT NULL,
    path              TEXT NOT NULL DEFAULT 'dagron',
    prune             BOOLEAN NOT NULL DEFAULT false,
    sync_requested_at TEXT,
    auth_kind         TEXT NOT NULL DEFAULT 'none',
    auth_username     TEXT,
    auth_secret       TEXT,
    auth_hint         TEXT,
    auth_known_hosts  TEXT,
    auth_updated_at   TEXT
);

ALTER TABLE workflows ADD COLUMN description TEXT;
ALTER TABLE workflows ADD COLUMN managed_by TEXT;
ALTER TABLE workflows ADD COLUMN managed_md5 TEXT;
ALTER TABLE workflow_runs ADD COLUMN triggered_by TEXT;

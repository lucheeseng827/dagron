-- GitOps repository registry (Postgres). Powers the UI's GitOps page: the set of
-- Git repos dagron tracks, each with sync state, current revision, and drift.
-- dagron-api owns this table (ensure_schema mirrors it); the engine does not read
-- it. Actual repo polling/reconcile is a follow-up — for now `state` is set by
-- connect + the Sync action.

CREATE TABLE IF NOT EXISTS git_repos (
    id              TEXT PRIMARY KEY NOT NULL,
    name            TEXT NOT NULL,          -- display "owner/repo"
    url             TEXT NOT NULL UNIQUE,
    branch          TEXT NOT NULL DEFAULT 'main',
    rev             TEXT,                   -- short commit sha
    state           TEXT NOT NULL DEFAULT 'OutOfSync',  -- Synced | OutOfSync | Syncing
    auto_sync       BIGINT NOT NULL DEFAULT 0,          -- 0/1
    workflow_count  BIGINT NOT NULL DEFAULT 0,
    drift           BIGINT NOT NULL DEFAULT 0,
    last_message    TEXT,                   -- latest commit subject
    last_synced_at  TEXT,
    created_at      TEXT NOT NULL
);

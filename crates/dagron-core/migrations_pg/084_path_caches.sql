-- 6.4 / 10.16: the index of path caches (a task's `workspace.cache`). The
-- bytes are in the object store the engine is given (DAGRON_CACHE_URL); a row
-- says where an entry is, how big, and when it was last read.
--
-- A row is `pending` from the moment a task is given a URL to save to, with
-- the task whose success makes it `ready`. The engine's cache sweep makes it
-- ready once that task has succeeded and the entry's manifest is in the store,
-- and drops it, with whatever was written, otherwise. A new table, so its
-- indexes are built with it.
CREATE TABLE IF NOT EXISTS path_caches (
    id           TEXT PRIMARY KEY,
    repo         TEXT NOT NULL,
    scope        TEXT NOT NULL,
    prefix       TEXT NOT NULL,
    version      TEXT NOT NULL,
    cache_key    TEXT,
    object_key   TEXT NOT NULL,
    size_bytes   BIGINT,
    state        TEXT NOT NULL,
    task_id      TEXT,
    created_at   TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_path_caches_lookup
    ON path_caches (scope, prefix, version, created_at) WHERE state = 'ready';
CREATE UNIQUE INDEX IF NOT EXISTS idx_path_caches_key
    ON path_caches (scope, prefix, version, cache_key) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS idx_path_caches_lru
    ON path_caches (repo, last_used_at) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS idx_path_caches_pending
    ON path_caches (created_at) WHERE state = 'pending';

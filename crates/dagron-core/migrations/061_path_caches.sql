-- 6.4 / 10.16: the index of path caches. See migrations_pg/084_path_caches.sql.
CREATE TABLE IF NOT EXISTS path_caches (
    id           TEXT PRIMARY KEY,
    repo         TEXT NOT NULL,
    scope        TEXT NOT NULL,
    prefix       TEXT NOT NULL,
    version      TEXT NOT NULL,
    cache_key    TEXT,
    object_key   TEXT NOT NULL,
    size_bytes   INTEGER,
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

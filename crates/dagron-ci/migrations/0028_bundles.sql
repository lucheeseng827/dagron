-- Patch bundles. See migrations_pg/0024_bundles.sql.
CREATE TABLE ci_bundles (
    id          TEXT PRIMARY KEY,
    repo_id     TEXT NOT NULL,
    base_sha    TEXT NOT NULL,
    principal   TEXT NOT NULL,
    object_name TEXT NOT NULL,
    size_bytes  INTEGER,
    sha256      TEXT,
    ready       INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at  TEXT NOT NULL
);
CREATE INDEX ci_bundles_expiry ON ci_bundles (expires_at);

ALTER TABLE ci_events ADD COLUMN bundle_id TEXT;

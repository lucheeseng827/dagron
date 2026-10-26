-- An index of the artifacts that went through the API, so that they can be
-- listed, deleted and expired. The bytes are in the artifact store, and were
-- never here. The API runs on Postgres, so nothing writes this table on
-- SQLite today: it is here so that the two backends keep one schema.
--
-- One row an artifact. `run_id`, `task_name` and `name` are the three parts
-- of its key as the store spells them (each part made safe for a path), so
-- a row and an object are one to one; `object_key` is those parts joined.
--
-- `size_bytes` is NULL for an upload that was begun and has not been said
-- to be complete. `sha256` is of the bytes as they passed through the API,
-- and NULL for an artifact that went straight to the store. `expires_at` is
-- NULL for an artifact that is kept. Times are text (RFC 3339, UTC).
CREATE TABLE IF NOT EXISTS artifacts (
    run_id      TEXT NOT NULL,
    task_name   TEXT NOT NULL,
    name        TEXT NOT NULL,
    object_key  TEXT NOT NULL,
    size_bytes  INTEGER,
    sha256      TEXT,
    expires_at  TEXT,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (run_id, task_name, name)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_expiry
    ON artifacts(expires_at) WHERE expires_at IS NOT NULL;

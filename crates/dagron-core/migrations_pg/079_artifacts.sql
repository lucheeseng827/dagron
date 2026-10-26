-- An index of the artifacts that went through the API, so that they can be
-- listed, deleted and expired. The bytes are in the artifact store, and were
-- never here.
--
-- One row an artifact. `run_id`, `task_name` and `name` are the three parts
-- of its key as the store spells them (each part made safe for a path), so
-- a row and an object are one to one; `object_key` is those parts joined.
--
-- `size_bytes` is NULL for an upload that was begun and has not been said
-- to be complete: the bytes were to go straight to the store, by a URL
-- signed for them, and nothing here saw them arrive. Such a row expires in
-- a day, and whatever did arrive goes with it.
--
-- `sha256` is of the bytes as they passed through the API. An artifact that
-- went straight to the store has none: it was not read here.
--
-- `expires_at` is NULL for an artifact that is kept. Times are text, as in
-- every table of the engine's (RFC 3339, UTC, so they order as text).
CREATE TABLE IF NOT EXISTS artifacts (
    run_id      TEXT NOT NULL,
    task_name   TEXT NOT NULL,
    name        TEXT NOT NULL,
    object_key  TEXT NOT NULL,
    size_bytes  BIGINT,
    sha256      TEXT,
    expires_at  TEXT,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (run_id, task_name, name)
);

-- What the sweep reads: the rows that expire, soonest first.
CREATE INDEX IF NOT EXISTS idx_artifacts_expiry
    ON artifacts(expires_at) WHERE expires_at IS NOT NULL;

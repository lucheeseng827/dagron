-- 3.3: the artifacts a workflow's jobs upload (actions/upload-artifact v4),
-- kept by the engine's results service. The bytes are in the object store the
-- engine is given (DAGRON_CACHE_URL); a row says whose they are, by run and
-- name, how big, their SHA-256 and when they expire.
--
-- A row is `pending` from the moment a job asks to upload, until it says the
-- upload is complete; one still pending when it expires (a day on) is
-- dropped, with whatever arrived. `artifact_id` is the number a job knows the
-- artifact by, below 2^53, as JavaScript reads it. A new table, so its indexes
-- are built with it.
CREATE TABLE IF NOT EXISTS workflow_artifacts (
    id           TEXT PRIMARY KEY,
    artifact_id  BIGINT NOT NULL,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    object_key   TEXT NOT NULL,
    size_bytes   BIGINT,
    sha256       TEXT,
    state        TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_artifacts_name
    ON workflow_artifacts (run_id, name) WHERE state = 'ready';
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_artifacts_number
    ON workflow_artifacts (artifact_id);
CREATE INDEX IF NOT EXISTS idx_workflow_artifacts_expiry
    ON workflow_artifacts (expires_at);

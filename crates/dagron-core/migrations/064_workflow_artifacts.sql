-- 3.3: the artifacts a workflow's jobs upload. See
-- migrations_pg/087_workflow_artifacts.sql.
CREATE TABLE IF NOT EXISTS workflow_artifacts (
    id           TEXT PRIMARY KEY,
    artifact_id  INTEGER NOT NULL,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    object_key   TEXT NOT NULL,
    size_bytes   INTEGER,
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

-- The credentials of tasks that reach their run's
-- artifacts over dagron-api's API (`artifact_api: true`). Written as such a
-- task is dispatched, in place of any its earlier attempts had; dagron-api
-- takes one while its task runs and it has not expired. Only the
-- credential's SHA-256 is kept.
CREATE TABLE IF NOT EXISTS artifact_api_tokens (
    token_sha256 TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_api_tokens_task ON artifact_api_tokens (task_id);
CREATE INDEX IF NOT EXISTS idx_artifact_api_tokens_expires ON artifact_api_tokens (expires_at);

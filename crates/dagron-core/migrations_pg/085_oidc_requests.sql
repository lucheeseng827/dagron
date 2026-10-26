-- 8.6: the request credentials of tasks that ask for an identity. The engine
-- writes one as it dispatches such a task, in place of any its earlier
-- attempts had; dagron-api signs a token for it while its task runs and it has
-- not expired. Only the credential's SHA-256 is kept. A new table, so its
-- indexes are built with it.
CREATE TABLE IF NOT EXISTS oidc_requests (
    token_sha256 TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    attempt      BIGINT NOT NULL,
    audiences    TEXT NOT NULL,
    environment  TEXT,
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oidc_requests_task ON oidc_requests (task_id);
CREATE INDEX IF NOT EXISTS idx_oidc_requests_expires ON oidc_requests (expires_at);

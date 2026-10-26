-- 8.6: the request credentials of tasks that ask for an identity. See
-- migrations_pg/085_oidc_requests.sql.
CREATE TABLE IF NOT EXISTS oidc_requests (
    token_sha256 TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    audiences    TEXT NOT NULL,
    environment  TEXT,
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oidc_requests_task ON oidc_requests (task_id);
CREATE INDEX IF NOT EXISTS idx_oidc_requests_expires ON oidc_requests (expires_at);

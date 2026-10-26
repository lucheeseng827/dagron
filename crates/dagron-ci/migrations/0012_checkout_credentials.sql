-- Mirrors migrations_pg/0012_checkout_credentials.sql: a row for each
-- checkout credential dagron-ci mints, written before the credential is
-- handed over, and marked when it is revoked. The comments there say what
-- it is for. It holds the credential's SHA-256, never the credential.
CREATE TABLE ci_checkout_credentials (
    id           TEXT PRIMARY KEY,
    token_sha256 TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    task_id      TEXT,
    attempt      INTEGER,
    repository   TEXT NOT NULL,
    commit_sha   TEXT NOT NULL,
    cell         TEXT NOT NULL,
    minted_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at   TEXT,
    revoked_at   TEXT
);
CREATE INDEX ci_checkout_credentials_token ON ci_checkout_credentials (token_sha256);
CREATE INDEX ci_checkout_credentials_run ON ci_checkout_credentials (run_id, minted_at);

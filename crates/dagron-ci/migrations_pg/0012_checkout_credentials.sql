-- A row for each checkout credential dagron-ci mints.
--
-- An engine asks for a credential before each attempt of a job whose
-- workspace has a checkout, and tells dagron-ci to revoke it when the attempt
-- is over. What said so was a log line and two counters. Asked "which
-- credential did this job hold, and when was it revoked?", an operator had
-- nothing to answer from.
--
-- One row a credential, written before the credential is handed over: no row,
-- no credential. It names what the credential was minted for. It does not
-- hold the credential: `token_sha256` is its SHA-256, which is how a revoke
-- finds its row, and from which the credential cannot be had.
--
-- The repository is kept as the URL the checkout fetched, and not as a
-- reference to `ci_repos`: disconnecting a repository drops its events and
-- its runs, and this record of what was handed out stays.
CREATE TABLE ci_checkout_credentials (
    id           TEXT PRIMARY KEY,
    token_sha256 TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    -- As the engine named them, which an engine need not.
    task_id      TEXT,
    attempt      BIGINT,
    repository   TEXT NOT NULL,
    commit_sha   TEXT NOT NULL,
    -- The cell the run was submitted to, whose engine asked.
    cell         TEXT NOT NULL,
    minted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- When the forge says it lapses on its own, in the forge's words.
    expires_at   TEXT,
    -- When dagron-ci revoked it, on the engine's word that its attempt was
    -- over. NULL on one that was not revoked: it lapsed at `expires_at`.
    revoked_at   TIMESTAMPTZ
);
CREATE INDEX ci_checkout_credentials_token ON ci_checkout_credentials (token_sha256);
CREATE INDEX ci_checkout_credentials_run ON ci_checkout_credentials (run_id, minted_at);

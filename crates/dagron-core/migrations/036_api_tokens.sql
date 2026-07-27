-- Personal access tokens: long-lived, named, revocable credentials for
-- non-browser clients (CI, the SDKs, scripts).
--
-- Before this, the only credential was the session JWT minted by /api/login,
-- so anything automated had to store a user's *password* and log in to get one.
-- A password cannot be scoped, cannot be revoked without locking the human out,
-- and leaves no trace of which client used it.
--
-- Only the SHA-256 of the token is stored, so a database dump does not yield
-- working credentials. SHA-256 rather than Argon2 on purpose: the secret is 32
-- random bytes, so there is no low-entropy guess for a KDF to slow down, and
-- this hash is computed on *every authenticated request* — an Argon2 verify
-- there (19 MiB, by design) would be a self-inflicted denial of service. The
-- `users.pw_hash` column stays Argon2 because a human password is exactly the
-- low-entropy case Argon2 exists for.
CREATE TABLE IF NOT EXISTS api_tokens (
    id           TEXT PRIMARY KEY NOT NULL,
    -- What the token is for, shown in the UI. Not unique: two CI jobs may
    -- reasonably both be called "ci".
    name         TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    -- Denormalised so a listing needs no join and an audit line survives the
    -- user row being renamed.
    user_email   TEXT NOT NULL,
    -- Cleartext head of the secret. Two jobs: the lookup key (so verification
    -- is one indexed row read, not a scan-and-compare over every token), and
    -- the disambiguator a human reads when deciding which token to revoke.
    prefix       TEXT NOT NULL,
    token_hash   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    -- NULL means the token does not expire. Allowed, because a CI credential
    -- that silently dies at 3am is its own outage, but the API offers an
    -- expiry and the UI should push toward one.
    expires_at   TEXT,
    -- Written at most once a minute (see routes::tokens::TOUCH_INTERVAL_SECS):
    -- the point is "is this still in use", not request-accurate telemetry, and
    -- a write per request would multiply the write load of every read.
    last_used_at TEXT,
    -- Revoked tokens are kept, not deleted: the row is the only record that the
    -- token ever existed, and that is what an audit needs after an incident.
    revoked_at   TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_tokens_prefix ON api_tokens(prefix);
CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(user_id);

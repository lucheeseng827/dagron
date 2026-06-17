-- dagron-api self-contained auth (Postgres). No external IdP / module_51.
--
-- dagron-api owns login: it verifies a password against `pw_hash` (argon2) and
-- mints its own HS256 session JWT (DAGRON_JWT_SECRET). The engine never reads
-- this table — only dagron-api does. `groups` is a JSON array of role strings,
-- copied into the token's claims.

-- `email ... UNIQUE` already creates a unique index Postgres uses for the
-- `WHERE email = $1` login lookup, so no separate index is needed.
CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    pw_hash     TEXT NOT NULL,
    groups      TEXT NOT NULL DEFAULT '[]',
    created_at  TEXT NOT NULL
);

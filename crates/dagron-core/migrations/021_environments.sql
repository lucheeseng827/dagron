-- Environments: named variable sets for workflow templating (`{{ env.NAME }}`)
-- plus write-only secrets, referenced from a spec via `environment: <name>`.
--
-- `variables` is a JSON object {name: value} of plain (templatable) values.
-- Secrets live in their own table, encrypted with AES-256-GCM under the
-- DAGRON_ENV_SECRET_KEY shared by dagron-api (encrypts) and the engine
-- (decrypts at dispatch); plaintext never touches the database.

CREATE TABLE IF NOT EXISTS environments (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    variables   TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS environment_secrets (
    environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    -- v1:<base64(nonce || ciphertext+tag)>, see dagron-crypto.
    ciphertext     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (environment_id, name)
);

-- Which environment a run was created with (NULL = none) — the engine reads
-- this at dispatch to resolve `value_from: {secret: ...}` against the
-- environment's secret store before falling back to process env / secrets dir.
ALTER TABLE workflow_runs ADD COLUMN environment TEXT;

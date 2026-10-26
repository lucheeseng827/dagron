-- Patch bundles: a change nobody pushed, as its uploader sent it,
-- for the run API to run (`crate::bundles`).
--
-- `ci_bundles`: each bundle, by its id: its repository and the base commit
-- it is laid over, who uploaded it (the only caller who may run it), where
-- CI_OBJECT_STORE_URL keeps it, and, once its bytes were checked, its size
-- and SHA-256 (`ready`). One uploaded through a link the store signs is not
-- ready until a run first names it. Each is deleted, object and row, at
-- `expires_at`, used or not.
CREATE TABLE ci_bundles (
    id          TEXT PRIMARY KEY,
    repo_id     TEXT NOT NULL,
    base_sha    TEXT NOT NULL,
    principal   TEXT NOT NULL,
    object_name TEXT NOT NULL,
    size_bytes  BIGINT,
    sha256      TEXT,
    ready       BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX ci_bundles_expiry ON ci_bundles (expires_at);

-- The bundle a run API request named: its runs are of the base, with the
-- bundle laid over it.
ALTER TABLE ci_events ADD COLUMN IF NOT EXISTS bundle_id TEXT;

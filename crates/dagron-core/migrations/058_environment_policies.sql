-- Who may use an environment (crate::envpolicy): the trust classes, the refs
-- and the pipelines whose runs it admits.
--
-- One row an environment, and none for most: an environment with no row has
-- the default policy, which admits `trusted` runs and no others. So a class
-- that is not `trusted` reads no environment until an operator writes a row
-- that names it.
--
-- Each list is a JSON array, as `environments.variables` is JSON: SQLite has
-- no array type, and the two backends keep one shape. `allowed_refs` and
-- `allowed_pipelines` are NULL for "any".
--
-- It goes with its environment (ON DELETE CASCADE), so an environment made
-- again under an old name starts with no policy, and with no secrets either.
CREATE TABLE IF NOT EXISTS environment_policies (
    environment_id    TEXT PRIMARY KEY NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    allowed_trust     TEXT NOT NULL DEFAULT '["trusted"]',
    allowed_refs      TEXT,
    allowed_pipelines TEXT,
    updated_at        TEXT NOT NULL,
    updated_by        TEXT
);

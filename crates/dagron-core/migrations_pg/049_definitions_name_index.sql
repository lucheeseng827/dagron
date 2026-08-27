-- no-transaction
-- Companion to 047 for the definition-name joins the runs listing and the
-- workflow-reference expansion make (`WHERE d.name = …` / `name = ANY(…)`).
-- Same care as 047: CONCURRENTLY in its own single-statement migration; an
-- interrupted build can leave an INVALID index that IF NOT EXISTS skips —
-- drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_definitions_name
    ON workflow_definitions (name);

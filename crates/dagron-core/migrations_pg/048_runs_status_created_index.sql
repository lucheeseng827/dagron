-- no-transaction
-- Companion to 047 for the status-filtered runs listing
-- (`GET /api/runs?status=…` orders the filtered set by `created_at DESC`).
-- Same care as 047: CONCURRENTLY in its own single-statement migration; an
-- interrupted build can leave an INVALID index that IF NOT EXISTS skips —
-- drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_status_created
    ON workflow_runs (status, created_at DESC);

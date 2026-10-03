-- The resolved `concurrency_key` of a run (NULL when the spec has none).
-- `max_active_runs` counts only running runs of the same workflow that share
-- the key, so a spec can cap "one run per stack" instead of "one run".
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS concurrency_key TEXT;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_concurrency_key
    ON workflow_runs (concurrency_key) WHERE status = 'running' AND concurrency_key IS NOT NULL;

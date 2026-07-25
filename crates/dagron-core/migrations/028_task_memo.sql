-- Result memoization store (spec fast-win #22 — Argo memoization / Prefect task
-- caching). A task with a `cache:` block records its successful output here,
-- keyed by (workflow name, task name, resolved cache key); a later task with the
-- same key reuses the output and skips execution. `created_at` backs the
-- optional `max_age_secs` expiry check at lookup time.
CREATE TABLE IF NOT EXISTS task_memo (
    workflow_name TEXT NOT NULL,
    task_name     TEXT NOT NULL,
    cache_key     TEXT NOT NULL,
    output        TEXT,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (workflow_name, task_name, cache_key)
);

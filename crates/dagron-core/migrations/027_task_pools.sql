-- Named concurrency pools (spec fast-win #21, second slice — Airflow #13975
-- "multiple pools per task" / Argo Kueue). A task may name a `pool`; a scheduler
-- claims a pooled task only while that pool has a free slot (running count <
-- the pool's capacity, configured out-of-band via the `POOLS` env, e.g.
-- `POOLS=etl:4,db:2`). Persisted per row so a retry / lease recovery stays in
-- its pool. NULL = unpooled = unlimited, so a deployment that configures no
-- pools behaves exactly as before.
ALTER TABLE task_runs ADD COLUMN pool TEXT;

-- Running-count-by-pool index for the claim's per-pool budget check (only the
-- in-flight rows, kept tiny by the partial predicate).
CREATE INDEX IF NOT EXISTS idx_task_runs_pool_running
    ON task_runs (pool)
    WHERE status = 'running' AND pool IS NOT NULL;

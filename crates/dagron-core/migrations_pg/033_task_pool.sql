-- Named concurrency pools (#21, second slice — Airflow #13975 / Argo Kueue). A
-- task may name a `pool`; a scheduler claims a pooled task only while the pool
-- has a free slot (running count < capacity, configured via the `POOLS` env).
-- NULL = unpooled = unlimited. Mirrors migrations/027_task_pools.sql (SQLite).
-- The running-count index lives in 034 as CREATE INDEX CONCURRENTLY (same split
-- as 027/028 and 022/023).
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS pool TEXT;

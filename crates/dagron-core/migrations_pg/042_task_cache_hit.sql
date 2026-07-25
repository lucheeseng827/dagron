-- Mirrors migrations/035_task_cache_hit.sql (SQLite): mark a task row resolved
-- from the memoization store (#22) instead of executed. BOOLEAN here vs SQLite's
-- INTEGER — sqlx maps both to `bool`.
--
-- Plain ADD COLUMN with a constant default: Postgres 11+ rewrites no rows for
-- this, so it is safe on a populated task_runs and needs no CONCURRENTLY dance.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN NOT NULL DEFAULT FALSE;

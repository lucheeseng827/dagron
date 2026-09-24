-- Mirrors migrations/045_runtime_fanout.sql (SQLite). See there for the full
-- reasoning: a fan-out over an upstream task's output cannot be resolved by the
-- expander, so the task is one parked row that a reconcile sweep expands and
-- that then stands as the join point its dependents were already wired to.
--
-- The index is split into 058 as CREATE INDEX CONCURRENTLY, the same split as
-- 053/054 and 031/032 — a plain CREATE INDEX here takes a lock that blocks
-- writers on a populated task_runs while migrations run at engine connect.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fanout_of     TEXT;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS fanout_parent TEXT;

-- A run that arrives while its workflow's cap is full may wait for a turn
-- (`when_full: queue`, or `supersede`): `queued_at` is when it began to wait,
-- and `admitted_at` when it was let in. It reads `running` meanwhile, with
-- its tasks held by one dependency more than they have. Mirrors
-- migrations/060_queued_runs.sql (SQLite), which says why. The index is 083,
-- on its own, so it can be built CONCURRENTLY.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS queued_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS admitted_at TEXT;

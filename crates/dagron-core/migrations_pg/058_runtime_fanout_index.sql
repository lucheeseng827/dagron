-- no-transaction
-- Index for 057, split out as CONCURRENTLY (see there, and 054).
--
-- Partial on `fanout_parent IS NOT NULL`: the column is NULL for every row in a
-- workflow that does not use a runtime fan-out, so this costs those runs
-- nothing and the sweep's "this barrier's instances" read is an index seek.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_fanout_parent
    ON task_runs(fanout_parent) WHERE fanout_parent IS NOT NULL;

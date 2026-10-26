-- 14.3: a pipeline that takes `on.failed` runs when another's run fails.
--
-- Whether the run's failure raises a `failed` event: set as the run is
-- recorded, where a pipeline of its commit names its pipeline in
-- `on.failed`, and the run is a trusted one's, of no patch bundle, and no
-- failure's own.
ALTER TABLE ci_runs ADD COLUMN on_failed BOOLEAN NOT NULL DEFAULT false;
-- When its `failed` event is due to be raised: set with its end, in the same
-- write, where it failed and `on_failed` holds; moved a lease on while it is
-- raised; NULL once the event is in the queue, in the same transaction.
ALTER TABLE ci_runs ADD COLUMN failed_due_at TIMESTAMPTZ;
CREATE INDEX ci_runs_failed_due ON ci_runs (failed_due_at) WHERE failed_due_at IS NOT NULL;

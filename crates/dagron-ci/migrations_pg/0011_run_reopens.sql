-- A run's record re-opens when the run runs again.
--
-- The engine can move a run after it has ended: a rerun, a retry, a clear.
-- dagron-ci's record moved once, to how the run first ended, and everything
-- that reads it took that for good: the checkout credential was refused, the
-- commit kept its old statuses, and the run held no slot.
--
-- When dagron-ci saw the run end. Set with the run's status, in the write
-- that ends it, and cleared when the record re-opens. A record re-opens only
-- on a sight of the run running that is later than this: one taken before
-- the end says nothing about after it. NULL on a run that has not ended, and
-- on one that ended before this column was.
ALTER TABLE ci_runs ADD COLUMN ended_at TIMESTAMPTZ;

-- How often the record re-opened. A job that failed, and fails again when its
-- run is run again, reads as it did: the count is what tells the reporter
-- that the result is a new one, to be posted again.
ALTER TABLE ci_runs ADD COLUMN reopens BIGINT NOT NULL DEFAULT 0;

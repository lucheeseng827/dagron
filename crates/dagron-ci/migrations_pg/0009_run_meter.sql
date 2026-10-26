-- What a meter (`Seams::run_meter`) is told of each run's end. Nothing here
-- is written while no meter is set, beyond `started`.
--
-- Whether any of the run's jobs started, as any look at the run saw: a job
-- whose attempt is above 0 was claimed by a scheduler. Once true it stays.
ALTER TABLE ci_runs ADD COLUMN started BOOLEAN NOT NULL DEFAULT false;
-- When the run's end is due to be told to the meter: set with its end, in the
-- same write, while a meter is set; moved a lease on while it is told; NULL
-- once the meter took it.
ALTER TABLE ci_runs ADD COLUMN meter_due_at TIMESTAMPTZ;
CREATE INDEX ci_runs_meter_due ON ci_runs (meter_due_at) WHERE meter_due_at IS NOT NULL;

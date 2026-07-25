-- Gang / co-scheduling (all-or-nothing claim for distributed training): a
-- `gang: {size: N}` task expands into N member rows at run creation, sharing
-- a gang_id. The gang-aware claimer (enterprise scheduler, RUNNER_GANGS=1)
-- claims a gang only when every member is ready and capacity fits the whole
-- gang; a member failure cancels its siblings (die-together). NULL = ordinary
-- task, all existing behaviour unchanged.
ALTER TABLE task_runs ADD COLUMN gang_id TEXT;
ALTER TABLE task_runs ADD COLUMN gang_rank BIGINT;
ALTER TABLE task_runs ADD COLUMN gang_size BIGINT;

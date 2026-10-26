-- The live relay: which text a task's log is. An attempt's
-- log is written again from its start where it is sealed (its whole text
-- once more, masked) or begun; a reader that follows it by a byte position
-- has to know that the bytes before its position are not the ones it read.
-- `log_gen` counts those writings from the start, and every one sets
-- `log_fence` to the attempt it is of: a log is the attempt it was written
-- under, which `version` stops saying where a teardown or a cancel takes the
-- row on. (Postgres has had `log_fence` since 094, for remote runners.)
--
-- NULL where no log was begun since this migration, which a reader takes as
-- 0 (and `version`).
ALTER TABLE task_runs ADD COLUMN log_fence INTEGER;
ALTER TABLE task_runs ADD COLUMN log_gen INTEGER;

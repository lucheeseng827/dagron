-- A task's log: stdout and stderr interleaved in arrival order, appended live by
-- executors that stream. Separate from `output` (stdout, the task's value that
-- `when:`, fan-out, `repeat.until` and the cache read), so a warning on stderr
-- cannot change what a condition sees. NULL for rows written before this column
-- and for executors that do not stream; log readers fall back to `output`.
ALTER TABLE task_runs ADD COLUMN log TEXT;

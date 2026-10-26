-- A rerun, a retry and a clear keep what they replace.
--
-- Each of them puts a task back to run again, and each wiped the attempt that
-- was there: its output, when it ran and how it ended. A task that failed, was
-- rerun and succeeded read as one that had succeeded, with nothing to say it
-- had ever done otherwise. `task_attempts` keeps the attempts a worker's own
-- retry supersedes, and this adds the ones a person supersedes.
--
-- `task_runs.attempt_base`: how many attempts the task made in the rounds
-- before this one. A rerun and a clear set `attempt` back to 0, so that the
-- task has its `max_attempts` again, and `task_attempts` is keyed by
-- (task, attempt): without this the second round's attempt 1 overwrote the
-- first round's. Those two add `attempt` to it as they zero it, and every row
-- of `task_attempts` is written under `attempt_base + attempt`: one count for
-- a task's life, which only rises, as the eviction of its oldest rows needs.
ALTER TABLE task_runs ADD COLUMN attempt_base INTEGER NOT NULL DEFAULT 0;

-- What the attempt was when it was replaced: `failed`, `cancelled`,
-- `succeeded` (a clear), `skipped`. NULL on the rows a worker's retry writes,
-- whose task was not terminal: `reason` says all there is to say of those.
-- `reason` gains `rerun`, `retry` and `clear`: what replaced the attempt.
ALTER TABLE task_attempts ADD COLUMN status TEXT;

-- When the attempt was queued, beside `finished_at`, which for these rows is
-- when the attempt ended and not when the row was written. NULL as above.
ALTER TABLE task_attempts ADD COLUMN scheduled_at TEXT;

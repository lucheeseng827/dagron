-- What a rerun leaves on record.
--
-- `task_runs.reset_by`: who put the task back to run again, by a rerun, a
-- retry or a clear, where dagron-api was asked by a person. An approval gate
-- that is put back asks again, and `not_triggerer` bars this person from
-- approving it as it bars the run's triggerer: else whoever re-armed a
-- production gate could approve it, the only person involved. NULL where the
-- task was never put back, or was put back by the engine's own API, which
-- names nobody.
ALTER TABLE task_runs ADD COLUMN reset_by TEXT;

-- `workflow_runs.whole_reruns`: how many times the run was rerun whole
-- (`from: all`). A rerun of every job is told apart from a rerun of the
-- failed ones by it, by a reader that counts runs.
ALTER TABLE workflow_runs ADD COLUMN whole_reruns INTEGER NOT NULL DEFAULT 0;

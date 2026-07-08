-- Run result (fast-win #15).
-- `result_from` names the task whose output becomes the run's result. It is set
-- at create_run from the spec's `result_from`; when the run succeeds,
-- `reap_completed_runs` copies that task's output into `workflow_runs.output`,
-- so a caller waiting on the run (POST /runs?wait=true / GET /runs/:id/wait)
-- gets a single return value. NULL = the run has no distinguished result.
ALTER TABLE workflow_runs ADD COLUMN result_from TEXT;

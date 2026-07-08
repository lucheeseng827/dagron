-- Per-task trigger rule (spec fast-win #10).
-- 'all_success' preserves the historical behavior (a task runs only when all
-- its dependencies succeeded; a failed dependency skips it). Other values
-- (all_done / one_failed / all_failed / none_failed) let a task be a cleanup
-- join or a failure handler. Evaluated by advance_ready_tasks once every
-- dependency is terminal.
ALTER TABLE task_runs ADD COLUMN trigger_rule TEXT NOT NULL DEFAULT 'all_success';

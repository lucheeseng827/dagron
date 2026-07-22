-- Runner-class routing (runner segmentation): which pool of scheduler replicas
-- may claim this task. Persisted per row so a retry / lease recovery stays in
-- its class. 'default' matches every task created before this migration and
-- every task that names no class, so an unsegmented deployment is unchanged.
ALTER TABLE task_runs ADD COLUMN runner_class TEXT NOT NULL DEFAULT 'default';

-- Class-scoped analog of idx_task_runs_ready_scheduled_at: a class-restricted
-- scheduler's claim scan stays tiny even when another class's backlog is deep.
CREATE INDEX IF NOT EXISTS idx_task_runs_ready_class
    ON task_runs (runner_class, scheduled_at)
    WHERE status = 'ready';

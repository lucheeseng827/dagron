-- What a rerun leaves on record: who put a task back to run again
-- (`task_runs.reset_by`), and how many times a run was rerun whole
-- (`workflow_runs.whole_reruns`). Mirrors migrations/072_rerun_records.sql
-- (SQLite), which says what each is for. A column with a constant default or
-- none, so neither rewrites its table.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS reset_by TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS whole_reruns BIGINT NOT NULL DEFAULT 0;

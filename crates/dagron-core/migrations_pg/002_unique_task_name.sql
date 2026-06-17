-- DB-level uniqueness guard: a run cannot have two task_runs with the same name.
-- Backs up the application-level check in db::create_run so bad DAGs cannot be
-- half-persisted even if the Rust guard is bypassed.
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_run_id_name
    ON task_runs(run_id, name);

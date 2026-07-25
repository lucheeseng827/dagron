-- Sub-workflow trigger (#23 — Airflow TriggerDagRunOperator / Argo #6922). A
-- `type: workflow` task submits a registered workflow as a child run and parks
-- until it is terminal. The parked task stays `running` with `claimed_by` /
-- `lease_expires_at` NULL and its child run id in `sub_run_id`, so neither the
-- claim scan nor lease recovery touch it — no new status value, no CHECK change.
-- Mirrors migrations/029_subworkflow.sql (SQLite). NULL = an ordinary task.
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS sub_run_id TEXT;

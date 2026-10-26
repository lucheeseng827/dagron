-- Mirrors migrations_pg/0032_on_failed.sql: a run whose failure raises a
-- `failed` event. The comments there say what each column is for.
ALTER TABLE ci_runs ADD COLUMN on_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ci_runs ADD COLUMN failed_due_at TEXT;
CREATE INDEX ci_runs_failed_due ON ci_runs (failed_due_at) WHERE failed_due_at IS NOT NULL;

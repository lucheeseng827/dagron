-- Mirrors migrations_pg/0009_run_meter.sql: what a meter is told of each
-- run's end. The comments there say what each column is for.
ALTER TABLE ci_runs ADD COLUMN started INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ci_runs ADD COLUMN meter_due_at TEXT;
CREATE INDEX ci_runs_meter_due ON ci_runs (meter_due_at) WHERE meter_due_at IS NOT NULL;

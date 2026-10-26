-- The parameters a run was created with, as a JSON object of strings. Mirrors
-- migrations/052_run_parameters.sql (SQLite), which says why. TEXT holding
-- JSON, like `event_outbox.payload`: dagron reads it whole and never queries
-- inside it.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS parameters TEXT;

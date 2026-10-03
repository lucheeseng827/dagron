-- Who submitted a run through the authenticated gateway (email, else subject).
-- NULL for runs nobody submitted interactively (schedules, dataset fires, the
-- engine API). Read by `not_triggerer` approval gates.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS triggered_by TEXT;

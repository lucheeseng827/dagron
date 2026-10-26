-- Mirrors migrations_pg/0007_commit_statuses.sql: where the commit-status
-- reporter is in each run. The comments there say what each column is for.
ALTER TABLE ci_runs ADD COLUMN report_due_at TEXT;
ALTER TABLE ci_runs ADD COLUMN report_claim TEXT;
ALTER TABLE ci_runs ADD COLUMN reported TEXT NOT NULL DEFAULT '{}';
ALTER TABLE ci_runs ADD COLUMN report_error TEXT;
CREATE INDEX ci_runs_report_due ON ci_runs (report_due_at) WHERE report_due_at IS NOT NULL;

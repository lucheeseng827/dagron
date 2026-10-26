-- 10.22: a run's record told of a change while a look of it was in hand.
-- See migrations_pg/0023_report_told.sql.
ALTER TABLE ci_runs ADD COLUMN report_told INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ci_runs ADD COLUMN report_looked INTEGER NOT NULL DEFAULT 0;
UPDATE ci_runs SET report_looked = 1 WHERE reported <> '{}';
ALTER TABLE ci_runs ADD COLUMN report_hold_until TEXT;
CREATE INDEX IF NOT EXISTS ci_runs_report_claim_order ON ci_runs (report_looked, report_due_at)
    WHERE report_due_at IS NOT NULL;

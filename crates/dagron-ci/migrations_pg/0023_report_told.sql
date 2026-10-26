-- 10.22: a run's record told of a change while a look of it was in hand.
-- The look read the run before the change, so it is looked at again as soon
-- as it is written (`finish_reports`), not at the time it set itself.
ALTER TABLE ci_runs ADD COLUMN IF NOT EXISTS report_told BOOLEAN NOT NULL DEFAULT false;

-- Whether a look of the run was ever written. A run that was not is claimed
-- before any other due (`CLAIM_REPORT_PG`): its jobs are to read pending
-- before they end. Not `reported = '{}'`, which stays so for a run whose
-- repository takes no statuses, or whose looks keep failing. A run with
-- statuses already posted has been looked at.
ALTER TABLE ci_runs ADD COLUMN IF NOT EXISTS report_looked BOOLEAN NOT NULL DEFAULT false;
UPDATE ci_runs SET report_looked = true WHERE reported <> '{}';

-- The soonest a notice makes the run due again: a couple of seconds after
-- each look, so that a run whose tasks change many times a second is looked
-- at once for them; and, after a look the forge rate-limited or that
-- failed, the wait it was given, which a notice does not cut short.
ALTER TABLE ci_runs ADD COLUMN IF NOT EXISTS report_hold_until TIMESTAMPTZ;

-- The claim's order: a run not looked at yet first, then the most overdue.
CREATE INDEX IF NOT EXISTS ci_runs_report_claim_order ON ci_runs (report_looked, report_due_at)
    WHERE report_due_at IS NOT NULL;

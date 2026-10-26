-- dagron-ci posts each run's commit statuses itself: one per job, pending
-- while it waits or runs, then its outcome. A reporter claims a run that is
-- due, reads it from its cell's dagron-api, posts what changed, and makes the
-- run due again while it runs.
--
-- When the reporter next looks at the run. Set when the run is recorded;
-- NULL once its statuses are final, or when they cannot be posted. A run
-- recorded before this migration has none, and is never reported.
ALTER TABLE ci_runs ADD COLUMN report_due_at TIMESTAMPTZ;
-- The reporter holding the run, which fences its write: one whose claim
-- lapsed and was taken over writes nothing.
ALTER TABLE ci_runs ADD COLUMN report_claim TEXT;
-- What was last posted under each context, as a JSON object of context to
-- `state:description`, so that a look posts only what changed.
ALTER TABLE ci_runs ADD COLUMN reported TEXT NOT NULL DEFAULT '{}';
-- Why the last look could not post, when it could not.
ALTER TABLE ci_runs ADD COLUMN report_error TEXT;
CREATE INDEX ci_runs_report_due ON ci_runs (report_due_at) WHERE report_due_at IS NOT NULL;

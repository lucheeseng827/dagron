-- Failure triage: a record that a human dealt with a failed run.
--
-- `status` says what the *engine* did; it has no way to say what a person then
-- decided. So a failed run stayed failed forever, and any "needs attention"
-- list built on it could only drain by the failure ageing out — which is not
-- triage, it is forgetting.
--
-- Deliberately not folded into the dead_letters table. That one holds
-- submissions that never became runs (payload/error/source), and a failed run
-- is a different animal: it has tasks, logs and its own recovery actions. One
-- table pretending to be both would serve neither.
--
-- Columns rather than a side table: there is exactly one triage record per run,
-- and the read that needs it — "failed and not yet triaged" — is the hot path
-- behind the overview. A join to answer that on every poll is a cost with
-- nothing to show for it.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS triage_state TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS triage_note  TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS triaged_at   TEXT;
-- Who decided. Email rather than id: the point of this column is to be readable
-- months later, when the account may be gone.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS triaged_by   TEXT;

-- The attention query is "failed runs with no triage yet", so index exactly
-- that. Partial, because triaged runs are the ones it never has to look at.
CREATE INDEX IF NOT EXISTS idx_runs_untriaged
    ON workflow_runs(status, created_at)
    WHERE triage_state IS NULL;

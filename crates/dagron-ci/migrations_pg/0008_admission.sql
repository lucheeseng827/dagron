-- A limit on runs in flight, for a build that sets one (`Seams::admission`):
-- before a worker submits a run, it takes a slot here, under the run's
-- idempotency key, while the runs already active leave room. The slot counts
-- against the limit while the run is submitted to dagron-api, and recording
-- the run in ci_runs deletes it in the same transaction, so a run is counted
-- once all the way. A slot whose worker stopped before recording lapses at
-- `expires_at`. Nothing writes here when no limit is set.
CREATE TABLE ci_admissions (
    idempotency_key TEXT PRIMARY KEY,
    expires_at      TIMESTAMPTZ NOT NULL
);
-- The runs a limit counts: submitted, and still followed by the reporter. A
-- run recorded before the reporter existed has no report_due_at; nothing
-- follows it to its end, so it is not counted.
CREATE INDEX ci_runs_active ON ci_runs (run_id)
    WHERE status = 'submitted' AND report_due_at IS NOT NULL;

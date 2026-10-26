-- Event workers. A worker claims an event under a lease, runs what the event
-- asks for, and leaves it `done`, `skipped` or `failed` with the reason, or
-- puts it back to be tried again later. A worker that dies mid-event loses
-- nothing: its lease runs out, and the next claim takes the event over.
ALTER TABLE ci_events ADD COLUMN claimed_by       TEXT;
ALTER TABLE ci_events ADD COLUMN claim_expires_at TIMESTAMPTZ;
ALTER TABLE ci_events ADD COLUMN attempts         INTEGER NOT NULL DEFAULT 0;
-- Set when an attempt failed in a way worth retrying: not claimed again before.
ALTER TABLE ci_events ADD COLUMN next_attempt_at  TIMESTAMPTZ;
-- Why the event ended as it did, or why its last attempt failed.
ALTER TABLE ci_events ADD COLUMN reason           TEXT;
-- Lease recovery reads expired claims; `ci_events_pending` serves new events.
CREATE INDEX ci_events_claimed ON ci_events (claim_expires_at) WHERE state = 'claimed';

-- What a repository's events run, until pipelines are compiled from the
-- repository itself: a workflow stored in dagron-api (its id), run on a cell
-- (a dagron-api named in CI_CELLS). A repository with no workflow runs nothing;
-- its events are skipped, with that as the reason.
ALTER TABLE ci_repos ADD COLUMN workflow TEXT;
ALTER TABLE ci_repos ADD COLUMN cell     TEXT NOT NULL DEFAULT 'default';

-- One row per run dagron-ci submitted: an event, a pipeline, an engine run.
-- `idempotency_key` derives from the event's commit key and the pipeline, and
-- is the Idempotency-Key the run was submitted under, so a webhook and a poll
-- that see one commit find this row and submit nothing more, and a worker that
-- died between submitting and writing this row gets the same run back.
-- `status` is the run's status as dagron-ci last saw it: `submitted` until
-- the reporter follows runs.
CREATE TABLE ci_runs (
    run_id          TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL,
    repo_id         TEXT NOT NULL REFERENCES ci_repos (id) ON DELETE CASCADE,
    cell            TEXT NOT NULL,
    pipeline        TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    frontend        TEXT NOT NULL CHECK (frontend IN ('native', 'github', 'gitlab')),
    status          TEXT NOT NULL DEFAULT 'submitted',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ci_runs_open ON ci_runs (created_at)
    WHERE status NOT IN ('succeeded', 'failed', 'cancelled');

-- What started a run, as the service that submitted it recorded it.
--
-- Until now a run's trigger was derived when it was read: a backfill ledger row
-- made it a backfill, a schedule stamp a schedule, and everything else read
-- "manual". A run a CI service submitted for a push was "manual" too, so no
-- list could be narrowed to CI's runs, and nothing on the run said which
-- repository, ref or commit it was for.
--
-- A side table, written in the run's own transaction and never updated. Only a
-- caller that may set it does (dagron-api: the `ci:trigger` group); a run with
-- no row here is derived as before. The row goes when its run goes.
--
-- Postgres only, as `workflow_runs.triggered_by` is (062): the row is written
-- by the authenticated gateway, which runs on Postgres.
CREATE TABLE IF NOT EXISTS run_triggers (
    run_id     TEXT PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE CASCADE,
    -- What kind of thing started the run. `ci` is the one kind written today.
    kind       TEXT NOT NULL,
    -- The authenticated submitter, set by the gateway and never by the caller:
    -- `ci:<its email or subject>`.
    principal  TEXT NOT NULL,
    -- Who caused it at the source: the pusher, the pull request's author, the
    -- person who asked for the run.
    actor      TEXT,
    -- What the submitter made of the event's origin: trusted, untrusted, agent.
    trust      TEXT,
    forge_host TEXT,
    -- owner/name on that forge.
    repo       TEXT,
    ref        TEXT,
    sha        TEXT,
    base_sha   TEXT,
    pr         BIGINT,
    -- The submitter's own id for the event, to find it again there.
    event_id   TEXT,
    created_at TEXT NOT NULL
);

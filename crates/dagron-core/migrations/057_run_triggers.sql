-- What started a run, as whatever started it recorded it.
--
-- Mirrors migrations_pg/072_run_triggers.sql and 077_run_trigger_source.sql
-- (Postgres), which made the table for the runs a CI service submits through
-- the gateway. Every run has one now, whatever made it: a schedule's fire, a
-- backfill, a dataset's update, a message of the engine's source, a parent
-- run's task, a call on the engine's own API. So the table is here too: an
-- engine on SQLite makes runs in all of those ways.
--
-- A side table, written in the run's own transaction and never updated. A run
-- with no row here predates it, and its trigger is derived when it is read as
-- it was. The row goes when its run goes.
CREATE TABLE IF NOT EXISTS run_triggers (
    run_id     TEXT PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE CASCADE,
    -- What kind of thing started the run: manual, api, schedule, backfill,
    -- dataset, workflow, source, ci.
    kind       TEXT NOT NULL,
    -- What submitted it. `engine` for a run the engine started itself, and
    -- `ops-api` for one asked of its own API, which has no login. A gateway
    -- sets it from the caller it authenticated: `user:<who>`, `ci:<who>`.
    principal  TEXT NOT NULL,
    -- Who caused it at the source: the person who asked, the pusher, a pull
    -- request's author. A parent run's, for a run its task started.
    actor      TEXT,
    -- What the submitter made of the event's origin: trusted, untrusted, agent.
    trust      TEXT,
    -- What it came from, where that is no repository: `schedule:<id>`,
    -- `cron:<name>`, `dataset:<uri>`, `run:<parent run>/<task>`,
    -- `source:<name>`, `dead-letter:<id>`.
    source     TEXT,
    forge_host TEXT,
    -- owner/name on that forge.
    repo       TEXT,
    ref        TEXT,
    sha        TEXT,
    base_sha   TEXT,
    pr         INTEGER,
    -- The id of the event at its source, to find it again there: a fire's
    -- time, a backfill's date, a source's position, a CI service's event.
    event_id   TEXT,
    created_at TEXT NOT NULL
);

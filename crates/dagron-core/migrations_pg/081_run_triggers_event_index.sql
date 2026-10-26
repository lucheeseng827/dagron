-- no-transaction
-- An event's runs.
--
-- A run's trigger record carries the id of the event at its source: a CI
-- service's event, which may have made a run for each of several pipelines.
-- `GET /api/runs?event=` finds an event's runs by it, which a run's page
-- asks for on every load. Partial: a run that came of no event has none.
--
-- CONCURRENTLY in its own single-statement migration, like 080, so building
-- it never blocks a run's creation. An interrupted build can leave an INVALID
-- index that IF NOT EXISTS skips; drop it and re-run migrations to rebuild.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_run_triggers_event
    ON run_triggers (event_id) WHERE event_id IS NOT NULL;

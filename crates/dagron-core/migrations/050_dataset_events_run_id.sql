-- The datasets a run produced, read by run.
--
-- `dataset_events` has only ever been read by dataset (`uri`, `id`): sensors and
-- triggers ask "has this dataset changed since my cursor". The OpenLineage
-- emitter asks the other question at every run's finalization — "which datasets
-- did this run produce" — so that it can name them as the run's outputs. Without
-- an index that is a full scan of the ledger on the finalization path.
--
-- Partial on `run_id IS NOT NULL`: an external event (`source = 'external'`)
-- belongs to no run and is never read this way.
CREATE INDEX IF NOT EXISTS idx_dataset_events_run_id
    ON dataset_events (run_id) WHERE run_id IS NOT NULL;

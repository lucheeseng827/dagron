-- Archive index (ee/STATE_STORE.md hot/cold split): one small row per run the
-- archive-before-purge GC exported, written just before the hot rows are
-- purged. This is what keeps archived history LISTABLE without scanning the
-- archive sink: dagron-api's /api/archive endpoints read this table and fetch
-- the run's JSON document from the sink on demand. `compacted_at`/`parquet_path`
-- are stamped by `dagron archive-compact` when the per-run document is folded
-- into the columnar dataset (after which the run is analytics-only).
CREATE TABLE IF NOT EXISTS archived_runs (
    run_id       TEXT PRIMARY KEY NOT NULL,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TEXT,
    finished_at  TEXT,
    archived_at  TEXT NOT NULL,
    compacted_at TEXT,
    parquet_path TEXT
);

-- The list endpoint pages newest-first by finish time.
CREATE INDEX IF NOT EXISTS idx_archived_runs_finished_at ON archived_runs (finished_at);

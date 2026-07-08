-- First-class backfill jobs (fast-win #18).
-- Unlike the synchronous `POST /schedules/:id/backfill` (which materializes the
-- whole window in one capped call), a `backfills` row is a durable, listable,
-- cancellable JOB that the engine's leadership-gated loop PACES: each tick it
-- fires the next few due fire-times, advances `cursor`, and marks the job
-- `completed` when `cursor` passes `range_to` or `fired` reaches `max_runs`.
-- Slots are still deduped through the existing `schedule_backfills` ledger
-- (keyed by schedule_id + logical_date), so a job never double-runs a slot a
-- manual backfill or catch-up already materialized.
CREATE TABLE IF NOT EXISTS backfills (
    id           TEXT PRIMARY KEY,
    schedule_id  TEXT NOT NULL,             -- ledger key (shared dedup w/ catch-up)
    cron_expr    TEXT NOT NULL,             -- snapshot at creation (stable if schedule changes)
    timezone     TEXT NOT NULL DEFAULT 'UTC',
    spec         TEXT NOT NULL,             -- denormalized workflow spec snapshot
    range_from   TEXT NOT NULL,             -- RFC-3339 inclusive lower bound
    range_to     TEXT NOT NULL,             -- RFC-3339 inclusive upper bound
    cursor       TEXT NOT NULL,             -- exclusive lower bound for the next enumeration
    status       TEXT NOT NULL DEFAULT 'running',  -- running | completed | cancelled
    max_runs     INTEGER NOT NULL,          -- hard cap on runs this job fires
    requested    INTEGER NOT NULL,          -- fire-times in range (progress denominator)
    fired        INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

-- The pacing loop scans only active jobs each tick.
CREATE INDEX IF NOT EXISTS idx_backfills_running
    ON backfills(status) WHERE status = 'running';

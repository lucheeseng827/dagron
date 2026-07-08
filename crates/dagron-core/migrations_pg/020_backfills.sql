-- First-class backfill jobs (fast-win #18). Mirrors migrations/017_backfills.sql.
CREATE TABLE IF NOT EXISTS backfills (
    id           TEXT PRIMARY KEY,
    schedule_id  TEXT NOT NULL,
    cron_expr    TEXT NOT NULL,
    timezone     TEXT NOT NULL DEFAULT 'UTC',
    spec         TEXT NOT NULL,
    range_from   TEXT NOT NULL,
    range_to     TEXT NOT NULL,
    cursor       TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running',
    max_runs     BIGINT NOT NULL,
    requested    BIGINT NOT NULL,
    fired        BIGINT NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backfills_running
    ON backfills(status) WHERE status = 'running';

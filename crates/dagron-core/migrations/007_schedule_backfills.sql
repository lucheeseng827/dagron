-- dagron QW3 — backfill dedup ledger (SQLite).
--
-- Mirrors migrations_pg/007_schedule_backfills.sql. One row per
-- (schedule, logical fire-time) a backfill has materialized, so re-issuing the
-- same backfill window cannot double-run a slot. The composite primary key is the
-- dedup gate (claimed via INSERT ... ON CONFLICT DO NOTHING). ON DELETE CASCADE
-- relies on `PRAGMA foreign_keys = ON` (set at pool init). Served only by
-- dagron-api (Postgres); created here for schema parity.

CREATE TABLE IF NOT EXISTS schedule_backfills (
    schedule_id   TEXT NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    logical_date  TEXT NOT NULL,
    run_id        TEXT,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (schedule_id, logical_date)
);

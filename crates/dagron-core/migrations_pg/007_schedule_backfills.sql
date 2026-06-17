-- dagron QW3 — backfill dedup ledger (Postgres).
--
-- Mirrors migrations/007_schedule_backfills.sql. One row per
-- (schedule, logical fire-time) a backfill has materialized, so re-issuing the
-- same `POST /api/schedules/:id/backfill` window cannot double-run a slot. The
-- composite primary key is the dedup gate: the backfill handler claims each slot
-- with `INSERT ... ON CONFLICT DO NOTHING` and only creates a run for slots it
-- newly claimed. `run_id` is filled in after the run is created (NULL until then,
-- and for slots claimed by a call whose create_run later failed).

CREATE TABLE IF NOT EXISTS schedule_backfills (
    schedule_id   TEXT NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    logical_date  TEXT NOT NULL,
    run_id        TEXT,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (schedule_id, logical_date)
);

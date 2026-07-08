-- dagron QW3 (auto-catchup) — per-schedule catch-up policy columns (SQLite).
--
-- Opts a schedule into automatic miss-detection (`catchup = 1`). The
-- window/cap columns override the engine's env defaults (NULL → use the
-- default). These are dynamically-typed INTEGER here (the Postgres mirror uses
-- BIGINT to match the i64 binding code).
--
-- The run_reruns attempt ledger and sweep hot-path indexes are applied only
-- when the `enterprise` Cargo feature is active.

ALTER TABLE schedules ADD COLUMN catchup INTEGER NOT NULL DEFAULT 0
    CHECK (catchup IN (0, 1));
ALTER TABLE schedules ADD COLUMN catchup_window_secs INTEGER
    CHECK (catchup_window_secs IS NULL OR catchup_window_secs >= 0);
ALTER TABLE schedules ADD COLUMN catchup_max_runs INTEGER
    CHECK (catchup_max_runs IS NULL OR catchup_max_runs >= 0);

-- dagron QW3 (auto-catchup) — per-schedule catch-up policy columns (Postgres).
--
-- Opts a schedule into automatic miss-detection (`catchup = 1`). The
-- window/cap columns override the engine's env defaults (NULL → use the
-- default). BIGINT to match the i64 binding code (the SQLite path stores
-- them in dynamically-typed INTEGER).
--
-- The run_reruns attempt ledger and sweep hot-path indexes are applied only
-- when the `enterprise` Cargo feature is active.

ALTER TABLE schedules ADD COLUMN IF NOT EXISTS catchup BIGINT NOT NULL DEFAULT 0
    CHECK (catchup IN (0, 1));
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS catchup_window_secs BIGINT
    CHECK (catchup_window_secs IS NULL OR catchup_window_secs >= 0);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS catchup_max_runs BIGINT
    CHECK (catchup_max_runs IS NULL OR catchup_max_runs >= 0);

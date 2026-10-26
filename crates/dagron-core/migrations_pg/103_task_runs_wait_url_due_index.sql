-- no-transaction
-- The HTTP sensors' poll (claim_due_url_waits) reads this index on every
-- pass: the `wait.url` tasks that are parked, by when each is next to be
-- polled. Partial, and of running rows alone: a sensor keeps its `wait_url`
-- once it has resolved, as every parked row keeps what parked it after it
-- has ended (only a reset clears them), and would stay in the index for as
-- long as its row is kept.
--
-- `status` leads, though on Postgres every row here is 'running': the SQLite
-- twin has the same text, and SQLite, which keeps no statistics of this
-- table, took idx_task_runs_status for the poll's read over a partial index
-- led by any other column. Mirrors migrations/076_sweep_indexes.sql (SQLite).
--
-- CONCURRENTLY in its own single-statement migration, like 101, so building
-- it never blocks a claim. An interrupted build can leave an INVALID index
-- that IF NOT EXISTS skips; the engine warns of one at start, and REINDEX
-- INDEX CONCURRENTLY rebuilds it.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_wait_url_due
    ON task_runs (status, next_poll_at)
    WHERE status = 'running' AND wait_url IS NOT NULL;

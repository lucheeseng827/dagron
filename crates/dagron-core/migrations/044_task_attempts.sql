-- Per-attempt output history (mirrors migrations_pg/056_task_attempts.sql).
--
-- `task_runs.output` is singular, and three writers take turns clobbering it:
-- `append_task_output` resets it on an attempt's first live chunk, `retry_task`
-- overwrites it when an iteration ends, `mark_task_succeeded_inner` overwrites
-- it at the end. So the log has only ever shown the *last* attempt — which for
-- a `repeat:` loop means 1 of N iterations, and for an ordinary retry means the
-- attempt that passed rather than the two that explain why it needed to.
--
-- This table holds the attempts that were superseded. The live one is NOT
-- copied here: it stays in `task_runs.output`, whole, streamed chunk by chunk,
-- which is what tailing a running task needs. One writer (`retry_task`, the
-- single point where an attempt's output is about to be destroyed), one reader.
--
-- Bounded on purpose, because the thing being multiplied is not bounded.
-- Executor output has no cap on the write path, and `RepeatSpec.max_iterations`
-- is a u32, and `GC_RETENTION_SECS` is unset (GC off) by default — retaining
-- iterations whole would multiply three unbounded quantities. `output` here is
-- a *tail*, capped at DAGRON_ATTEMPT_LOG_BYTES, and rows are capped per task at
-- DAGRON_ATTEMPT_LOG_KEEP. Both are constants known before the run starts, so
-- the added storage is a number the budget could refuse at submit rather than a
-- surprise halfway through. See docs/ITERATION-LOGS.md for the measurements.
CREATE TABLE IF NOT EXISTS task_attempts (
    -- ON DELETE CASCADE is load-bearing, not tidiness: `foreign_keys` is ON
    -- for this backend and the retention sweep does a bare
    -- `DELETE FROM task_runs WHERE run_id = ?` (two call sites, plus the
    -- Postgres pair). A plain reference would turn every purge of a run that
    -- contained a loop into a foreign-key violation, wedging GC — and it would
    -- do so only once something had actually iterated. The cascade also means
    -- no purge path has to learn this table exists.
    task_id     TEXT NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    -- 1-based, and already correct with no bookkeeping: `attempt` is
    -- incremented at claim time, so when retry_task runs the row's `attempt` is
    -- the number of the attempt that just finished.
    attempt     INTEGER NOT NULL,
    -- Why this attempt ended, in the vocabulary the reader already knows:
    -- 'iteration' = a `repeat:` pass whose `until` did not hold yet,
    -- 'failed'    = an attempt that errored and is being retried.
    -- Deliberately not the task's own status enum — the task is not terminal,
    -- and reusing 'failed'/'succeeded' here would read as if it were.
    reason      TEXT NOT NULL,
    -- The tail. NULL when the attempt printed nothing.
    output      TEXT,
    -- Recorded, not inferred: a reader must be able to say "there was more"
    -- without having to know what the cap was when this row was written.
    truncated   INTEGER NOT NULL DEFAULT 0,
    finished_at TEXT NOT NULL,
    PRIMARY KEY (task_id, attempt)
);

-- The read is always "this task's attempts, oldest first", and the eviction
-- sweep is "this task's oldest attempt" — both served by the primary key's
-- leading column. No separate index: it would be redundant with the PK and pay
-- a write on every iteration of every loop.

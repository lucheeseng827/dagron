-- no-transaction
-- The tasks a session holds, found by the session. Partial on
-- `running`, as the session sweep and a session's claim ask of them alone.
-- CONCURRENTLY (so its own no-transaction, single-statement migration, as 023
-- is) so building it on a populated task_runs never blocks a live engine's
-- claims and marks. An interrupted build can leave an INVALID index that IF
-- NOT EXISTS then skips: drop it and run the migrations again.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_session
    ON task_runs (session_id)
    WHERE status = 'running';
